import asyncio
import time
import secrets
from collections import deque, OrderedDict
from typing import Dict, Union, Optional, Tuple, List
import traceback
from fastapi import Request
from pyrogram import Client, raw, utils
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth
from Backend.logger import LOGGER
from Backend.helper.exceptions import FIleNotFound
from Backend.helper.pyro import get_file_ids
from Backend.pyrofork.bot import work_loads, multi_clients



ACTIVE_STREAMS: Dict[str, Dict] = {}
RECENT_STREAMS = deque(maxlen=3)

class ChunkCache:
    def __init__(self, max_size_mb: int = 500):
        self.max_size = max_size_mb * 1024 * 1024
        self.current_size = 0
        self._cache = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[bytes]:
        async with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        return None

    async def set(self, key: str, data: bytes):
        if not data: return
        size = len(data)
        
        async with self._lock:
            # If already exists, update and move to end
            if key in self._cache:
                self.current_size -= len(self._cache[key])
                self.current_size += size
                self._cache[key] = data
                self._cache.move_to_end(key)
            else:
                self._cache[key] = data
                self.current_size += size
            
            # Evict if too big
            while self.current_size > self.max_size:
                start_size = self.current_size # prevention loop
                k, v = self._cache.popitem(last=False)
                self.current_size -= len(v)
                if self.current_size == start_size: break # Safety

GLOBAL_CACHE = ChunkCache(max_size_mb=500)


class StreamPipeline:
    """Manages a shared streaming pipeline for a file.
    
    Multiple concurrent HTTP requests for the same file share the same pipeline,
    ensuring consistent bot selection, shared queue, and proper resource management.
    """
    def __init__(self, stream_id: str, queue: asyncio.Queue, stop_event: asyncio.Event,
                 client_index: int, additional_client_indices: List[int]):
        self.stream_id = stream_id
        self.queue = queue
        self.stop_event = stop_event
        self.client_index = client_index
        self.additional_client_indices = additional_client_indices
        self.ref_count = 0
        self.producer_task: Optional[asyncio.Task] = None
        self.error: Optional[Exception] = None
        self.created_at = time.time()
        
    def all_client_indices(self) -> List[int]:
        """Returns all bot indices (primary + helpers)"""
        return [self.client_index] + self.additional_client_indices


# Global registry of active streaming pipelines
STREAM_PIPELINES: Dict[str, StreamPipeline] = {}
PIPELINE_LOCK = asyncio.Lock()


async def _ensure_sessions_ready(
    primary_session,
    file_id: FileId,
    additional_client_indices: List[int],
    stream_id: str,
    max_wait: float = 2.0  # Reduced from 10.0 since pre-warming happens at startup
) -> List:
    """
    Collects ready bot sessions for the producer.
    Sessions are pre-warmed at application startup, so this is just a safety check.
    
    Returns: List of ready sessions [primary, helper1, helper2, ...]
    """
    session_pool = [primary_session]
    
    for idx in additional_client_indices:
        try:
            cli = multi_clients[idx]
            
            # Quick check - sessions should already be ready from startup pre-warming
            if file_id.dc_id in getattr(cli, "media_sessions", {}):
                session_pool.append(cli.media_sessions[file_id.dc_id])
                LOGGER.debug(f"Stream {stream_id[:8]}: Bot{idx} session ready")
            else:
                # Wait briefly if not immediately available
                wait_interval = 0.1
                waited = 0
                while waited < max_wait:
                    if file_id.dc_id in getattr(cli, "media_sessions", {}):
                        session_pool.append(cli.media_sessions[file_id.dc_id])
                        LOGGER.debug(f"Stream {stream_id[:8]}: Bot{idx} session ready after {waited:.1f}s")
                        break
                    await asyncio.sleep(wait_interval)
                    waited += wait_interval
                else:
                    LOGGER.warning(f"Stream {stream_id[:8]}: Bot{idx} session not ready after {max_wait}s (pre-warming may have failed)")
        except Exception as e:
            LOGGER.warning(f"Stream {stream_id[:8]}: Bot{idx} error: {e}")
    
    return session_pool


async def _producer_task(
    pipeline: StreamPipeline,
    file_id: FileId,
    session_pool: List,
    location,
    offset: int,
    part_count: int,
    chunk_size: int,
    parallelism: int,
    stream_id: str
):
    """
    Producer task - fetches chunks and puts them in queue.
    Only ONE instance per file, shared by all consumers.
    """
    q = pipeline.queue
    stop_event = pipeline.stop_event
    inflight_tracker = {id(s): 0 for s in session_pool}
    
    async def fetch_chunk(seq_idx: int, off: int) -> Tuple[int, Optional[bytes]]:
        """Fetch single chunk with retry logic"""
        # Check cache first
        cache_key = f"{file_id.dc_id}:{getattr(file_id, 'volume_id', 0)}:{getattr(file_id, 'local_id', 0)}:{off}"
        cached = await GLOBAL_CACHE.get(cache_key)
        if cached:
            return seq_idx, cached
        
        tries = 0
        max_retries = 10
        
        while tries < max_retries and not stop_event.is_set():
            # Least-loaded session selection
            session = min(session_pool, key=lambda s: inflight_tracker.get(id(s), 0))
            inflight_tracker[id(session)] += 1
            
            try:
                r = await session.send(
                    raw.functions.upload.GetFile(location=location, offset=off, limit=chunk_size)
                )
                chunk_bytes = getattr(r, "bytes", None) if r else None
                
                if chunk_bytes:
                    await GLOBAL_CACHE.set(cache_key, chunk_bytes)
                    
                    # Log every 10th chunk for debugging
                    if seq_idx % 10 == 0:
                        session_idx = session_pool.index(session) if session in session_pool else -1
                        LOGGER.debug(f"Stream {stream_id[:8]}: Chunk {seq_idx} by session {session_idx}")
                
                return seq_idx, chunk_bytes
            except Exception as e:
                tries += 1
                if "RPCError" in str(e):
                    LOGGER.warning(f"Chunk {seq_idx} error (try {tries}/{max_retries}): {getattr(e, 'NAME', e)}")
                await asyncio.sleep(0.15 * tries)
            finally:
                if id(session) in inflight_tracker:
                    inflight_tracker[id(session)] -= 1
        
        LOGGER.error(f"Failed chunk {seq_idx} after {max_retries} retries")
        return seq_idx, None
    
    try:
        if part_count <= 0:
            await q.put((None, None))
            return
        
        # Parallel fetching with buffer
        next_to_schedule = 0
        scheduled_tasks = {}
        results_buffer = {}
        next_to_put = 0
        max_parallel = max(1, parallelism)
        
        # Schedule initial batch
        for i in range(min(part_count, max_parallel)):
            seq = next_to_schedule
            off = offset + seq * chunk_size
            task = asyncio.create_task(fetch_chunk(seq, off))
            scheduled_tasks[seq] = task
            next_to_schedule += 1
        
        # Main fetch loop
        while next_to_put < part_count:
            if stop_event.is_set():
                break
            
            if not scheduled_tasks:
                break
            
            done, _ = await asyncio.wait(scheduled_tasks.values(), return_when=asyncio.FIRST_COMPLETED)
            
            for completed in done:
                completed_seq = None
                for k, t in list(scheduled_tasks.items()):
                    if t is completed:
                        completed_seq = k
                        break
                
                if completed_seq is None:
                    continue
                
                seq_idx, chunk_bytes = completed.result()
                scheduled_tasks.pop(completed_seq, None)
                
                if chunk_bytes is None:
                    LOGGER.error(f"Empty chunk {seq_idx} - aborting stream")
                    pipeline.error = Exception("Chunk fetch failed")
                    await q.put((None, None))
                    return
                
                results_buffer[seq_idx] = chunk_bytes
                
                # Schedule next chunk
                if next_to_schedule < part_count:
                    seq = next_to_schedule
                    off = offset + seq * chunk_size
                    task = asyncio.create_task(fetch_chunk(seq, off))
                    scheduled_tasks[seq] = task
                    next_to_schedule += 1
            
            # Put sequential chunks in queue
            while next_to_put in results_buffer:
                chunk_bytes = results_buffer.pop(next_to_put)
                await q.put((offset + next_to_put * chunk_size, chunk_bytes))
                next_to_put += 1
        
        await q.put((None, None))
    
    except asyncio.CancelledError:
        LOGGER.debug(f"Producer cancelled for {stream_id[:8]}")
        try:
            await q.put((None, None))
        except:
            pass
        raise
    except Exception as e:
        LOGGER.exception(f"Producer error for {stream_id[:8]}: {e}")
        pipeline.error = e
        try:
            await q.put((None, None))
        except:
            pass


async def _consumer_generator(
    pipeline: StreamPipeline,
    request: Optional[Request],
    first_part_cut: int,
    last_part_cut: int,
    part_count: int,
    stream_id: str,
    should_update_stats: bool
):
    """
    Consumer generator - reads from shared queue and yields chunks.
    Multiple instances can exist (one per HTTP request).
    Only the creator updates ACTIVE_STREAMS stats.
    """
    q = pipeline.queue
    current_part_idx = 1
    
    try:
        while True:
            # Check disconnection
            try:
                if request and await request.is_disconnected():
                    LOGGER.debug(f"Client disconnected for {stream_id[:8]}")
                    if should_update_stats and stream_id in ACTIVE_STREAMS:
                        ACTIVE_STREAMS[stream_id]["status"] = "cancelled"
                    break
            except:
                pass
            
            # Get chunk from queue
            off_chunk = await q.get()
            if off_chunk is None or off_chunk == (None, None):
                break
            
            off, chunk = off_chunk
            if off is None and chunk is None:
                break
            
            # Update stats (only pipeline creator)
            if should_update_stats and stream_id in ACTIVE_STREAMS:
                try:
                    entry = ACTIVE_STREAMS[stream_id]
                    chunk_len = len(chunk) if chunk else 0
                    now_ts = time.time()
                    
                    elapsed = now_ts - entry["last_ts"]
                    if elapsed <= 0:
                        elapsed = 1e-6
                    
                    recent = entry["recent_measurements"]
                    recent.append((chunk_len, elapsed))
                    
                    if len(recent) >= 2:
                        total_bytes = sum(b for b, _ in recent)
                        total_time = sum(t for _, t in recent)
                        instant_mbps = min((total_bytes / (1024 * 1024)) / max(total_time, 0.01), 1000.0)
                    else:
                        instant_mbps = 0.0
                    
                    entry["total_bytes"] += chunk_len
                    entry["last_ts"] = now_ts
                    
                    total_time = now_ts - entry["start_ts"]
                    if total_time <= 0:
                        total_time = 1e-6
                    
                    entry["avg_mbps"] = (entry["total_bytes"] / (1024 * 1024)) / total_time
                    entry["instant_mbps"] = instant_mbps
                    
                    if instant_mbps > entry["peak_mbps"]:
                        entry["peak_mbps"] = instant_mbps
                except Exception as e:
                    LOGGER.warning(f"Stat update error: {e}")
            
            # Yield chunk with appropriate cuts
            if part_count == 1:
                yield chunk[first_part_cut:last_part_cut]
            elif current_part_idx == 1:
                yield chunk[first_part_cut:]
            elif current_part_idx == part_count:
                yield chunk[:last_part_cut]
            else:
                yield chunk
            
            current_part_idx += 1
    
    except asyncio.CancelledError:
        LOGGER.debug(f"Consumer cancelled for {stream_id[:8]}")
        if should_update_stats and stream_id in ACTIVE_STREAMS:
            ACTIVE_STREAMS[stream_id]["status"] = "cancelled"
        raise
    except Exception as e:
        LOGGER.exception(f"Consumer error for {stream_id[:8]}: {e}")
        if should_update_stats and stream_id in ACTIVE_STREAMS:
            ACTIVE_STREAMS[stream_id]["status"] = "error"
        raise



class ByteStreamer:
    CHUNK_SIZE = 1024 * 1024  # 1 MB
    CLEAN_INTERVAL = 30 * 60  # 30 minutes

    def __init__(self, client: Client):
        self.client = client
        self._file_id_cache: Dict[int, FileId] = {}
        self._session_lock = asyncio.Lock()
        asyncio.create_task(self._clean_cache())
        asyncio.create_task(self._prewarm_sessions())

    async def _prewarm_sessions(self):
        common_dcs = [1, 2, 4, 5]  # Main Telegram DCs
        LOGGER.debug("Pre-warming media sessions for common DCs...")
        
        for dc in common_dcs:
            try:
                if dc in self.client.media_sessions:
                    LOGGER.debug(f"Media session for DC {dc} already exists, skipping")
                    continue

                test_mode = await self.client.storage.test_mode()
                current_dc = await self.client.storage.dc_id()
 
                if dc == current_dc:
                    continue
                
                auth_key = await Auth(self.client, dc, test_mode).create()
                session = Session(self.client, dc, auth_key, test_mode, is_media=True)
                session.no_updates = True
                session.timeout = 30
                session.sleep_threshold = 60
                
                await session.start()
                
                for attempt in range(6):
                    try:
                        exported = await self.client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=dc)
                        )
                        await session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported.id, bytes=exported.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        LOGGER.debug(f"AuthBytesInvalid during pre-warm for DC {dc}; retrying...")
                        await asyncio.sleep(0.5)
                    except OSError:
                        LOGGER.debug(f"OSError during pre-warm for DC {dc}; retrying...")
                        await asyncio.sleep(1)
                    except Exception as e:
                        LOGGER.debug(f"Error during pre-warm for DC {dc}: {e}")
                        break
                
                self.client.media_sessions[dc] = session
                LOGGER.debug(f"Pre-warmed media session for DC {dc}")
                
            except Exception as e:
                LOGGER.debug(f"Could not pre-warm DC {dc}: {e}")
                continue

    async def get_file_properties(self, chat_id: int, message_id: int) -> FileId:
        if message_id not in self._file_id_cache:
            file_id = await get_file_ids(self.client, int(chat_id), int(message_id))
            if not file_id:
                LOGGER.warning("Message %s not found", message_id)
                raise FIleNotFound
            self._file_id_cache[message_id] = file_id
        return self._file_id_cache[message_id]

    async def prefetch_stream(
        self,
        file_id: FileId,
        client_index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
        prefetch: int = 3,
        stream_id: Optional[str] = None,
        meta: Optional[dict] = None,
        parallelism: int = 2,
        request: Optional[Request] = None,
        additional_client_indices: List[int] = [],
    ):
        """
        Streaming with Global Pipeline Manager.
        
        Same stream_id → Same pipeline → Same bots → Same queue
        """
        if not stream_id:
            stream_id = secrets.token_hex(8)
        
        pipeline = None
        is_pipeline_creator = False
        
        try:
            # === PHASE 1: Get or Create Pipeline ===
            async with PIPELINE_LOCK:
                if stream_id in STREAM_PIPELINES:
                    # Reuse existing
                    pipeline = STREAM_PIPELINES[stream_id]
                    pipeline.ref_count += 1
                    LOGGER.info(f"Stream {stream_id[:8]}: Reusing pipeline (ref={pipeline.ref_count})")
                else:
                    # Create new
                    is_pipeline_creator = True
                    
                    q = asyncio.Queue(maxsize=max(1, prefetch))
                    stop_event = asyncio.Event()
                    
                    pipeline = StreamPipeline(
                        stream_id=stream_id,
                        queue=q,
                        stop_event=stop_event,
                        client_index=client_index,
                        additional_client_indices=additional_client_indices
                    )
                    pipeline.ref_count = 1
                    
                    # Register in ACTIVE_STREAMS
                    now = time.time()
                    ACTIVE_STREAMS[stream_id] = {
                        "stream_id": stream_id,
                        "msg_id": getattr(file_id, "local_id", None),
                        "chat_id": getattr(file_id, "chat_id", None),
                        "dc_id": file_id.dc_id,
                        "client_index": client_index,
                        "additional_indices": additional_client_indices,
                        "start_ts": now,
                        "last_ts": now,
                        "total_bytes": 0,
                        "avg_mbps": 0.0,
                        "instant_mbps": 0.0,
                        "peak_mbps": 0.0,
                        "recent_measurements": deque(maxlen=5),
                        "status": "active",
                        "part_count": part_count,
                        "prefetch": prefetch,
                        "meta": meta or {},
                    }
                    
                    # Increment workloads ONCE
                    work_loads[client_index] += 1
                    for idx in additional_client_indices:
                        work_loads[idx] += 1
                    
                    STREAM_PIPELINES[stream_id] = pipeline
                    LOGGER.info(f"Stream {stream_id[:8]}: NEW pipeline (bots={pipeline.all_client_indices()})")
            
            # === PHASE 2: Pre-warm Sessions (Creator Only) ===
            if is_pipeline_creator:
                media_session = await self._get_media_session(file_id)
                location = await self._get_location(file_id)
                
                # BLOCKING session pre-warming
                session_pool = await _ensure_sessions_ready(
                    media_session,
                    file_id,
                    additional_client_indices,
                    stream_id
                )
                
                LOGGER.info(f"Stream {stream_id[:8]}: {len(session_pool)}/{len(additional_client_indices)+1} sessions ready!")
                
                # START producer
                pipeline.producer_task = asyncio.create_task(
                    _producer_task(
                        pipeline, file_id, session_pool, location,
                        offset, part_count, chunk_size, parallelism, stream_id
                    )
                )
            
            # === PHASE 3: Consume (All Requests) ===
            async for chunk in _consumer_generator(
                pipeline, request, first_part_cut, last_part_cut,
                part_count, stream_id, is_pipeline_creator
            ):
                yield chunk
        
        finally:
            # === PHASE 4: Cleanup ===
            if pipeline:
                async with PIPELINE_LOCK:
                    pipeline.ref_count -= 1
                    LOGGER.debug(f"Stream {stream_id[:8]}: Exit (ref={pipeline.ref_count})")
                    
                    if pipeline.ref_count <= 0:
                        # Last consumer
                        LOGGER.info(f"Stream {stream_id[:8]}: Last consumer - cleanup")
                        
                        pipeline.stop_event.set()
                        
                        if pipeline.producer_task and not pipeline.producer_task.done():
                            pipeline.producer_task.cancel()
                            try:
                                await asyncio.wait_for(pipeline.producer_task, timeout=2.0)
                            except:
                                pass
                        
                        # Decrement workloads (last consumer cleans up)
                        try:
                            work_loads[pipeline.client_index] -= 1
                            for idx in pipeline.additional_client_indices:
                                if idx in work_loads and work_loads[idx] > 0:
                                    work_loads[idx] -= 1
                        except:
                            pass
                        
                        # Move to RECENT_STREAMS
                        if stream_id in ACTIVE_STREAMS:
                            try:
                                entry = ACTIVE_STREAMS[stream_id]
                                end_ts = time.time()
                                entry.update({
                                    "end_ts": end_ts,
                                    "duration": end_ts - entry["start_ts"],
                                    "status": entry.get("status", "finished"),
                                })
                                RECENT_STREAMS.appendleft(ACTIVE_STREAMS.pop(stream_id))
                            except:
                                pass
                        
                        STREAM_PIPELINES.pop(stream_id, None)


    async def _get_media_session(self, file_id: FileId) -> Session:
        dc = file_id.dc_id
        media_session = self.client.media_sessions.get(dc)

        if media_session:
            return media_session

        async with self._session_lock:
            media_session = self.client.media_sessions.get(dc)
            if media_session:
                return media_session

            test_mode = await self.client.storage.test_mode()
            current_dc = await self.client.storage.dc_id()

            if dc != current_dc:
                auth_key = await Auth(self.client, dc, test_mode).create()
            else:
                auth_key = await self.client.storage.auth_key()

            session = Session(self.client, dc, auth_key, test_mode, is_media=True)
            session.no_updates = True
            session.timeout = 30 
            session.sleep_threshold = 60 

            await session.start()

            if dc != current_dc:
                for _ in range(6):
                    try:
                        exported = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc))
                        await session.send(raw.functions.auth.ImportAuthorization(id=exported.id, bytes=exported.bytes))
                        break
                    except AuthBytesInvalid:
                        LOGGER.debug("AuthBytesInvalid during media session import; retrying...")
                        await asyncio.sleep(0.5)
                    except OSError:
                        LOGGER.debug("OSError during media session import; retrying...")
                        await asyncio.sleep(1)

            self.client.media_sessions[dc] = session
            LOGGER.debug("Created media session for DC %s", dc)
            return session

    @staticmethod
    async def _get_location(file_id: FileId) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        ftype = file_id.file_type

        if ftype == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(user_id=file_id.chat_id, access_hash=file_id.chat_access_hash)
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(channel_id=utils.get_channel_id(file_id.chat_id),
                                                    access_hash=file_id.chat_access_hash)

            return raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )

        if ftype == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        return raw.types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    async def _clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.CLEAN_INTERVAL)
            self._file_id_cache.clear()
            LOGGER.debug("ByteStreamer: cleared file_id cache")
