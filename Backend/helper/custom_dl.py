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
        if not stream_id:
            stream_id = secrets.token_hex(8)

        now = time.time()
        registry_entry = {
            "stream_id": stream_id,
            "msg_id": getattr(file_id, "local_id", None) or None,
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

        # Only register and increment workloads if this is a NEW stream
        # Multiple concurrent HTTP requests for the same file will have the same stream_id
        is_new_stream = stream_id not in ACTIVE_STREAMS
        
        if is_new_stream:
            ACTIVE_STREAMS[stream_id] = registry_entry
            work_loads[client_index] += 1
            for idx in additional_client_indices:
                 work_loads[idx] += 1

        try:
            queue_maxsize = max(1, prefetch)
            q: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
            stop_event = asyncio.Event()

            media_session = await self._get_media_session(file_id)
            location = await self._get_location(file_id)
            
            # Build Session Pool
            session_pool = [media_session]
            for idx in additional_client_indices:
                try:
                    cli = multi_clients[idx]
                    # Only usage helper if session exists (no blocking pre-warm here)
                    if file_id.dc_id in getattr(cli, "media_sessions", {}):
                         session_pool.append(cli.media_sessions[file_id.dc_id])
                except Exception:
                    pass
            
            # Debug: Log session pool details
            LOGGER.info(f"Stream {stream_id[:8]}: Built session pool with {len(session_pool)} sessions for DC{file_id.dc_id}")
            if len(session_pool) < len(additional_client_indices) + 1:
                LOGGER.warning(f"Stream {stream_id[:8]}: Only {len(session_pool)}/{len(additional_client_indices)+1} sessions ready (some helpers not pre-warmed yet)")
            
            LOGGER.debug(f"Stream {stream_id}: Using {len(session_pool)} sessions for DC {file_id.dc_id}")

            # Load Balancing: Track pending requests per session
            # using object id as key
            inflight_tracker = {id(s): 0 for s in session_pool}
            
            async def fetch_chunk_with_retries(seq_idx: int, off: int) -> Tuple[int, Optional[bytes]]:
                # 1. Check Cache First
                cache_key = f"{file_id.dc_id}:{getattr(file_id, 'volume_id', 0)}:{getattr(file_id, 'local_id', 0)}:{off}"
                cached_data = await GLOBAL_CACHE.get(cache_key)
                if cached_data:
                    return seq_idx, cached_data

                tries = 0
                max_retries = 10
                last_error = None
                
                while tries < max_retries and not stop_event.is_set():
                    # Smart Selection: Pick session with least pending requests
                    # If retrying, this naturally might pick a different session if the original one is stuck/loaded
                    session = min(session_pool, key=lambda s: inflight_tracker.get(id(s), 0))
                    inflight_tracker[id(session)] += 1
                    
                    try:
                        r = await session.send(
                            raw.functions.upload.GetFile(location=location, offset=off, limit=chunk_size)
                        )
                        chunk_bytes = getattr(r, "bytes", None) if r else None
                        
                        # 2. Store in Cache
                        if chunk_bytes:
                            await GLOBAL_CACHE.set(cache_key, chunk_bytes)
                            
                        return seq_idx, chunk_bytes
                    except Exception as e:
                        tries += 1
                        last_error = e
                        wait_time = 0.15 * tries 
                        
                        if "RPCError" in str(e): 
                             LOGGER.warning(
                                "Fetch chunk error seq=%s off=%s try=%s/%s err=%s. Retrying on new session...",
                                seq_idx, off, tries, max_retries, getattr(e, "NAME", e)
                            )
                        await asyncio.sleep(wait_time)
                    finally:
                        # Release load count
                        if id(session) in inflight_tracker:
                            inflight_tracker[id(session)] -= 1
                
                LOGGER.error("Failed to fetch chunk seq=%s off=%s after %s retries. Last error: %s", 
                            seq_idx, off, max_retries, last_error)
                return seq_idx, None

            async def producer():
                try:
                    if part_count <= 0:
                        await q.put((None, None))
                        return

                    next_to_schedule = 0
                    scheduled_tasks = {}
                    results_buffer = {}
                    next_to_put = 0
                    max_parallel = max(1, parallelism)

                    initial = min(part_count, max_parallel)
                    for i in range(initial):
                        seq = next_to_schedule
                        off = offset + seq * chunk_size
                        task = asyncio.create_task(fetch_chunk_with_retries(seq, off))
                        scheduled_tasks[seq] = task
                        next_to_schedule += 1

                    while next_to_put < part_count:
                        if stop_event.is_set():
                            break

                        if not scheduled_tasks:
                            seq = next_to_schedule
                            off = offset + seq * chunk_size
                            task = asyncio.create_task(fetch_chunk_with_retries(seq, off))
                            scheduled_tasks[seq] = task
                            next_to_schedule += 1

                        done, _ = await asyncio.wait(scheduled_tasks.values(), return_when=asyncio.FIRST_COMPLETED)

                        for completed in done:
                            try:
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
                                    LOGGER.error("Chunk fetch returned empty for stream=%s seq=%s", stream_id, seq_idx)
                                    await q.put((None, None))
                                    return

                                results_buffer[seq_idx] = chunk_bytes

                                if next_to_schedule < part_count:
                                    seq = next_to_schedule
                                    off = offset + seq * chunk_size
                                    task = asyncio.create_task(fetch_chunk_with_retries(seq, off))
                                    scheduled_tasks[seq] = task
                                    next_to_schedule += 1

                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                LOGGER.exception("Error processing completed fetch task: %s", e)
                                await q.put((None, None))
                                return

                        while next_to_put in results_buffer:
                            chunk_bytes = results_buffer.pop(next_to_put)
                            await q.put((offset + next_to_put * chunk_size, chunk_bytes))
                            next_to_put += 1

                    await q.put((None, None))

                except asyncio.CancelledError:
                    LOGGER.debug("Producer cancelled for stream %s", stream_id)
                    try:
                        await q.put((None, None))
                    except Exception:
                        pass
                    raise
                except Exception as e:
                    LOGGER.exception("Producer unexpected error for stream %s: %s", stream_id, e)
                    try:
                        await q.put((None, None))
                    except Exception:
                        pass

        except Exception:
            if is_new_stream:
                work_loads[client_index] -= 1
                for idx in additional_client_indices:
                    if idx in work_loads and work_loads[idx] > 0:
                        work_loads[idx] -= 1
            if stream_id in ACTIVE_STREAMS:
                del ACTIVE_STREAMS[stream_id]
            raise

        async def consumer_generator():
            producer_task = asyncio.create_task(producer())
            current_part_idx = 1

            try:
                while True:
                    try:
                        if request and await request.is_disconnected():
                            LOGGER.debug("Client disconnected for stream %s; cancelling stream", stream_id)
                            ACTIVE_STREAMS[stream_id]["status"] = "cancelled"
                            break
                    except Exception:
                        pass

                    off_chunk = await q.get()
                    if off_chunk is None:
                        break

                    off, chunk = off_chunk
                    if off is None and chunk is None:
                        break

                    try:
                        chunk_len = len(chunk)
                    except Exception:
                        chunk_len = 0

                    now_ts = time.time()
                    elapsed = now_ts - ACTIVE_STREAMS[stream_id]["last_ts"]
                    if elapsed <= 0:
                        elapsed = 1e-6

                    recent = ACTIVE_STREAMS[stream_id]["recent_measurements"]
                    recent.append((chunk_len, elapsed))

                    if len(recent) >= 2:
                        total_bytes = sum(b for b, _ in recent)
                        total_time = sum(t for _, t in recent)
                        instant_mbps = min((total_bytes / (1024 * 1024)) / max(total_time, 0.01), 1000.0)
                    else:
                        instant_mbps = 0.0

                    ACTIVE_STREAMS[stream_id]["total_bytes"] += chunk_len
                    ACTIVE_STREAMS[stream_id]["last_ts"] = now_ts

                    total_time = now_ts - ACTIVE_STREAMS[stream_id]["start_ts"]
                    if total_time <= 0:
                        total_time = 1e-6

                    ACTIVE_STREAMS[stream_id]["avg_mbps"] = (ACTIVE_STREAMS[stream_id]["total_bytes"] / (1024 * 1024)) / total_time
                    ACTIVE_STREAMS[stream_id]["instant_mbps"] = instant_mbps

                    if instant_mbps > ACTIVE_STREAMS[stream_id]["peak_mbps"]:
                        ACTIVE_STREAMS[stream_id]["peak_mbps"] = instant_mbps

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
                LOGGER.debug("Consumer cancelled for stream %s", stream_id)
                if not producer_task.done():
                    producer_task.cancel()
                ACTIVE_STREAMS[stream_id]["status"] = "cancelled"
                raise
            except Exception as e:
                LOGGER.exception("Consumer error for stream %s: %s", stream_id, e)
                ACTIVE_STREAMS[stream_id]["status"] = "error"
                if not producer_task.done():
                    producer_task.cancel()
                raise
            finally:
                if not producer_task.done():
                    try:
                        producer_task.cancel()
                        await asyncio.wait_for(producer_task, timeout=2.0)
                    except Exception:
                        pass

                try:
                    end_ts = time.time()
                    total_bytes = ACTIVE_STREAMS[stream_id]["total_bytes"]
                    start_ts = ACTIVE_STREAMS[stream_id]["start_ts"]
                    duration = end_ts - start_ts if end_ts > start_ts else 0.0
                    avg_mbps = (total_bytes / (1024 * 1024)) / (duration if duration > 0 else 1e-6)

                    entry = ACTIVE_STREAMS.get(stream_id, {})
                    entry.update({
                        "end_ts": end_ts,
                        "duration": duration,
                        "avg_mbps": avg_mbps,
                        "status": entry.get("status", "finished"),
                        "parallelism": parallelism,
                    })

                    try:
                        RECENT_STREAMS.appendleft(ACTIVE_STREAMS.pop(stream_id))
                    except KeyError:
                        pass
                finally:
                    # Only decrement if we were the ones who incremented (is_new_stream)
                    if is_new_stream:
                        try:
                            work_loads[client_index] -= 1
                            for idx in additional_client_indices:
                                 if idx in work_loads and work_loads[idx] > 0:
                                      work_loads[idx] -= 1
                        except Exception:
                            pass

                stop_event.set()

        return consumer_generator()

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
