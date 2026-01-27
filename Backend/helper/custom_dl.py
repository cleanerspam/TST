import asyncio
import time
import secrets
from collections import deque, OrderedDict
from typing import Dict, Union, Optional, Tuple, List
import traceback
from fastapi import Request
from pyrogram import Client, raw, utils
from pyrogram.errors import AuthBytesInvalid, FileReferenceExpired, FloodWait, AuthKeyInvalid, FileIdInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth
from Backend.logger import LOGGER
from Backend.helper.exceptions import FIleNotFound
from Backend.helper.pyro import get_file_ids
from Backend.pyrofork.bot import work_loads, multi_clients


def get_session_name(session) -> str:
    """
    Extract readable name from Session or Client object.
    
    Handles both direct Client objects and Session wrapper objects,
    providing consistent naming across all log messages.
    """
    try:
        if hasattr(session, 'name'):
            return str(session.name)
        if hasattr(session, 'client') and hasattr(session.client, 'name'):
            return str(session.client.name)
        return f"Session-{id(session) % 10000}"
    except Exception:
        return "Unknown"


class CircuitBreaker:
    """
    Prevents repeated attempts on permanently failed chunks.
    
    After a chunk fails 'threshold' times, it enters an "open" state
    where further attempts are blocked for 'timeout' seconds.
    This prevents wasting bandwidth on chunks that will never succeed.
    """
    def __init__(self, threshold: int = 3, timeout: int = 60):
        self.failures = {}  # key -> (count, last_attempt_time)
        self.threshold = threshold
        self.timeout = timeout
        
    def is_open(self, key: str) -> bool:
        """Returns True if circuit is open (should NOT attempt)"""
        if key not in self.failures:
            return False
        count, last_time = self.failures.get(key, (0, 0))
        # Reset after timeout
        if time.time() - last_time > self.timeout:
            del self.failures[key]
            return False
        return count >= self.threshold
        
    def record_failure(self, key: str):
        """Record a failure for this key"""
        count, _ = self.failures.get(key, (0, 0))
        self.failures[key] = (count + 1, time.time())
        
    def record_success(self, key: str):
        """Clear failures for this key on success"""
        self.failures.pop(key, None)


ACTIVE_STREAMS: Dict[str, Dict] = {}
LOCATION_CACHE = OrderedDict()
LOCATION_CACHE_TTL = 3000 # 50 minutes
RECENT_STREAMS = deque(maxlen=3)

class ChunkCache:
    """
    Enhanced chunk cache with stream pinning and metrics.
    
    Pinned streams are protected from eviction, ensuring active playback
    doesn't suffer from cache thrashing.
    """
    def __init__(self, max_size_mb: int = 500):
        self.max_size = max_size_mb * 1024 * 1024
        self.current_size = 0
        self._cache = OrderedDict()  # key -> {'data': bytes, 'stream_id': str, 'pinned': bool}
        self._lock = asyncio.Lock()
        
        # Stream pinning
        self._pinned_streams = set()
        
        # Metrics
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def pin_stream(self, stream_id: str):
        """Protect chunks from this stream from eviction"""
        self._pinned_streams.add(stream_id)
        
    def unpin_stream(self, stream_id: str):
        """Allow chunks from this stream to be evicted"""
        self._pinned_streams.discard(stream_id)

    async def get(self, key: str) -> Optional[bytes]:
        async with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]['data']
            self._misses += 1
        return None

    async def set(self, key: str, data: bytes, stream_id: Optional[str] = None):
        if not data: return
        size = len(data)
        
        async with self._lock:
            # Determine if this chunk should be pinned
            is_pinned = stream_id in self._pinned_streams if stream_id else False
            
            # Store with metadata
            entry = {
                'data': data,
                'stream_id': stream_id,
                'pinned': is_pinned
            }
            
            # If already exists, update and move to end
            if key in self._cache:
                self.current_size -= len(self._cache[key]['data'])
                self.current_size += size
                self._cache[key] = entry
                self._cache.move_to_end(key)
            else:
                self._cache[key] = entry
                self.current_size += size
            
            # Evict ONLY unpinned chunks if too big
            while self.current_size > self.max_size:
                evicted = False
                for k, v in list(self._cache.items()):
                    if not v.get('pinned', False):
                        # Found unpinned chunk to evict
                        evicted_size = len(v['data'])
                        self._cache.pop(k)
                        self.current_size -= evicted_size
                        self._evictions += 1
                        evicted = True
                        break
                
                if not evicted:
                    # All remaining chunks are pinned, stop evicting
                    LOGGER.warning(
                        f"Cache full with only pinned chunks "
                        f"({self.current_size / (1024*1024):.1f}MB/{self.max_size / (1024*1024):.1f}MB, "
                        f"{len(self._pinned_streams)} pinned streams)"
                    )
                    break

    def get_stats(self) -> dict:
        """Return cache performance metrics"""
        total_requests = self._hits + self._misses
        hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0
        return {
            'hit_rate': f"{hit_rate:.1f}%",
            'size_mb': self.current_size / (1024 * 1024),
            'items': len(self._cache),
            'evictions': self._evictions,
            'pinned_streams': len(self._pinned_streams)
        }

    def get_stream_stats(self, stream_id: str) -> dict:
        """Return cache stats for a specific stream"""
        cached_bytes = 0
        chunks = 0
        
        # We don't lock for this read-only iteration to avoid blocking writers
        for v in self._cache.values():
            if v.get('stream_id') == stream_id:
                cached_bytes += len(v['data'])
                chunks += 1
                
        return {
            'cached_mb': cached_bytes / (1024 * 1024),
            'chunks': chunks
        }

# Initialize cache with config value (fallback to 500MB if not set)
try:
    from Backend.config import Telegram
    GLOBAL_CACHE = ChunkCache(max_size_mb=getattr(Telegram, 'CACHE_SIZE_MB', 500))
    LOGGER.info(f"Initialized cache with {getattr(Telegram, 'CACHE_SIZE_MB', 500)}MB")
except Exception as e:
    LOGGER.warning(f"Failed to load CACHE_SIZE_MB from config, using default 500MB: {e}")
    GLOBAL_CACHE = ChunkCache(max_size_mb=500)


class StreamPipeline:
    """Manages a shared streaming pipeline for a file.
    
    Multiple concurrent HTTP requests for the same file share the same pipeline,
    ensuring consistent bot selection, shared queue, and proper resource management.
    """
    def __init__(self, client_index: int, additional_client_indices: List[int], start_offset: int, queue_size: int = 20):
        self.stop_event = asyncio.Event()
        # Enhancement 7: Configurable queue size (default 20 for stable buffering)
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.client_index = client_index
        self.additional_client_indices = additional_client_indices
        self.start_offset = start_offset
        self.producer_task: Optional[asyncio.Task] = None
        self.ref_count = 0
        self.error: Optional[Exception] = None
        self.delayed_cleanup_task: Optional[asyncio.Task] = None
        LOGGER.debug(f"DEBUG: Pipeline created for client_index={client_index} helpers={additional_client_indices} queue_size={queue_size}")
        
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
                # Success - session was pre-warmed (expected case, no log needed)
            else:
                # Unexpected - session not ready, log warning
                LOGGER.warning(f"Stream {stream_id[:8]}: Bot{idx} session NOT ready for DC{file_id.dc_id}. Waiting...")
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
    stream_id: str,
    byte_streamer = None,
    chat_id: int = 0,
    message_id: int = 0
):
    """
    Producer task - fetches chunks and puts them in queue.
    Only ONE instance per file, shared by all consumers.
    """
    q = pipeline.queue
    stop_event = pipeline.stop_event
    inflight_tracker = {id(s): 0 for s in session_pool}
    circuit_breaker = CircuitBreaker()  # Track permanent failures
    
    async def fetch_chunk(seq_idx: int, off: int) -> Tuple[int, Optional[bytes]]:
        """Fetch single chunk with retry logic"""
        nonlocal location
        LOGGER.debug(f"DEBUG [{stream_id[:8]}]: fetch_chunk called seq={seq_idx} off={off}")
        
        # Check cache first
        cache_key = f"{file_id.dc_id}:{getattr(file_id, 'volume_id', 0)}:{getattr(file_id, 'local_id', 0)}:{off}"
        
        # Check circuit breaker BEFORE attempting fetch
        if circuit_breaker.is_open(cache_key):
            LOGGER.error(f"[{stream_id[:8]}]: Circuit breaker OPEN for chunk {seq_idx} - skipping (too many failures)")
            return seq_idx, None
        
        cached = await GLOBAL_CACHE.get(cache_key)
        if cached:
            LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Chunk {seq_idx} CACHE HIT")
            circuit_breaker.record_success(cache_key)  # Cache hit = success
            return seq_idx, cached
        
        LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Chunk {seq_idx} CACHE MISS")
        
        # Setup multi-bot location tracking if not already done
        client_map = {id(c): k for k, c in multi_clients.items()}
        primary_bot_idx = client_map.get(id(byte_streamer.client)) if byte_streamer else 0
        
        # Current valid locations per bot
        bot_locations = {primary_bot_idx: location}
        
        async def get_bot_location(idx):
             if idx in bot_locations: return bot_locations[idx]
             
             # Check Global Cache
             cache_key_loc = f"{idx}:{chat_id}:{message_id}"
             if cache_key_loc in LOCATION_CACHE:
                 loc, ts = LOCATION_CACHE[cache_key_loc]
                 if time.time() - ts < LOCATION_CACHE_TTL:
                     bot_locations[idx] = loc
                     return loc
             
             try:
                 client = multi_clients.get(idx)
                 if not client or not chat_id or not message_id: return None
                 fid = await get_file_ids(client, chat_id, message_id)
                 loc = await ByteStreamer._get_location(fid)
                 
                 # Update caches
                 bot_locations[idx] = loc
                 LOCATION_CACHE[cache_key_loc] = (loc, time.time())
                 if len(LOCATION_CACHE) > 500:
                     LOCATION_CACHE.popitem(last=False)
                     
                 return loc
             except Exception as e:
                 LOGGER.warning(f"Failed to fetch location for Bot{idx}: {e}")
                 return None

        def get_bot_idx_from_session(s):
             if hasattr(s, 'invoke'): return client_map.get(id(s)) # Client
             return client_map.get(id(s.client)) if hasattr(s, 'client') else None
        
        tries = 0
        refresh_attempts = 0
        max_retries = 10
        max_refresh_attempts = 6
        
        while tries < max_retries and not stop_event.is_set():
            # Least-loaded session selection
            session = min(session_pool, key=lambda s: inflight_tracker.get(id(s), 0))
            inflight_tracker[id(session)] += 1
            
            # Determine which bot this is
            bot_idx = get_bot_idx_from_session(session)
            
            # Get valid location for THIS bot
            current_location = await get_bot_location(bot_idx)
            if not current_location:
                 # Fallback/Fail for this bot, try next session
                 tries += 1
                 continue

            # Log first attempt of first chunk to verify location/session
            if seq_idx == 0 and tries == 0:
                LOGGER.info(f"Fetching Chunk 0 using Bot{get_session_name(session)}")
            
            LOGGER.debug(f"DEBUG [{stream_id[:8]}]: seq={seq_idx} off={off} try={tries} session={get_session_name(session)}")
            
            try:
                # Handle both Client (main bot) and Session (helper) objects
                async with asyncio.timeout(15) if hasattr(asyncio, "timeout") else asyncio.wait_for(asyncio.sleep(0), 15): # Fallback/Compatibility hack, wait.. direct wait_for is better
                    pass

                # Cleaner implementation using try/except around the call
                if hasattr(session, "invoke"):
                    # It's a Client object (Home DC)
                    r = await asyncio.wait_for(session.invoke(
                         raw.functions.upload.GetFile(location=current_location, offset=off, limit=chunk_size)
                    ), timeout=15)
                else:
                    # It's a Session object (Helper DC)
                    r = await asyncio.wait_for(session.send(
                        raw.functions.upload.GetFile(location=current_location, offset=off, limit=chunk_size)
                    ), timeout=15)

                chunk_bytes = getattr(r, "bytes", None) if r else None
                
                if chunk_bytes:
                    await GLOBAL_CACHE.set(cache_key, chunk_bytes, stream_id=stream_id)
                    circuit_breaker.record_success(cache_key)  # Successfully fetched
                    
                    if seq_idx == 0:
                         LOGGER.info(f"Stream {stream_id[:8]}: Chunk 0 SUCCEEDED ({len(chunk_bytes)} bytes)")

                    # Log every 10th chunk for debugging
                    if seq_idx % 10 == 0:
                        LOGGER.debug(f"Stream {stream_id[:8]}: Chunk {seq_idx} by Bot{get_session_name(session)}")
                
                return seq_idx, chunk_bytes
            
            except FileReferenceExpired:
                # File reference expired for this SPECIFIC bot
                if bot_idx is not None:
                    bot_locations.pop(bot_idx, None) # Invalidate local
                    LOCATION_CACHE.pop(f"{bot_idx}:{chat_id}:{message_id}", None) # Invalidate global
                
                refresh_attempts += 1
                if refresh_attempts >= max_refresh_attempts:
                     LOGGER.error(f"Stream {stream_id[:8]}: Max refresh attempts ({max_refresh_attempts}) reached")
                     circuit_breaker.record_failure(cache_key)
                     tries += 1
                     continue

                LOGGER.warning(f"Stream {stream_id[:8]}: FileReferenceExpired for Bot{bot_idx}, refreshing...")
                
                try:
                    if bot_idx is not None and chat_id and message_id:
                         client = multi_clients.get(bot_idx)
                         await asyncio.sleep(0.5)
                         # Use helper to get ID for THIS client
                         new_file_id = await get_file_ids(client, chat_id, message_id) 
                         new_location = await ByteStreamer._get_location(new_file_id)
                         
                         # Update caches
                         bot_locations[bot_idx] = new_location
                         LOCATION_CACHE[f"{bot_idx}:{chat_id}:{message_id}"] = (new_location, time.time())
                         
                         LOGGER.info(f"Stream {stream_id[:8]}: File reference refreshed successfully for Bot{bot_idx}")
                         tries = 0 
                         continue
                except Exception as e:
                     LOGGER.error(f"Refresh failed for Bot{bot_idx}: {e}")
                
                tries += 1
                continue

            
            except FloodWait as e:
                # Telegram rate limiting - wait with cap
                wait_time = min(e.value, 30)  # Cap at 30s per chunk to avoid excessive buffering
                LOGGER.warning(f"[{stream_id[:8]}]: FloodWait {wait_time}s for chunk {seq_idx}")
                await asyncio.sleep(wait_time)
                tries += 1
            
            except (AuthKeyInvalid, FileIdInvalid) as e:
                # Permanent errors - mark with circuit breaker and abort
                LOGGER.error(f"[{stream_id[:8]}]: Permanent error for chunk {seq_idx}: {type(e).__name__}")
                circuit_breaker.record_failure(cache_key)
                return seq_idx, None  # Abort immediately
            
            except Exception as e:
                # Generic errors - use LINEAR delay to reduce buffering
                tries += 1
                circuit_breaker.record_failure(cache_key)
                # LINEAR delay: 0.15s, 0.30s, 0.45s, 0.60s, ... up to 1.5s max
                delay = min(0.15 * tries, 1.5)
                print(f"CRITICAL: Chunk {seq_idx} FAILED: {type(e).__name__}: {e}") # Force output
                LOGGER.error(f"[{stream_id[:8]}]: Chunk {seq_idx} FAILED (try {tries}/{max_retries}): {type(e).__name__}: {e}")
                LOGGER.debug(f"[{stream_id[:8]}]: Retry delay: {delay:.2f}s")
                await asyncio.sleep(delay)
            
            finally:
                if id(session) in inflight_tracker:
                    inflight_tracker[id(session)] -= 1
        
        if stop_event.is_set():
            LOGGER.debug(f"DEBUG: Producer for stream {stream_id[:8]} stopped cleanly.")
            return seq_idx, None

        # All retries exhausted
        LOGGER.error(f"[{stream_id[:8]}]: Chunk {seq_idx} failed after {max_retries} retries")
        circuit_breaker.record_failure(cache_key)
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
        max_parallel = max(2, min(len(session_pool), parallelism))  # Use at least 2, up to available sessions
        
        # Enhancement 6: Producer Watchdog - detect stuck tasks
        WATCHDOG_TIMEOUT = 120  # 2 minutes max per chunk batch
        
        # Enhancement 12: Periodic ref count check - adaptive frequency
        if part_count < 50:  # Small files (<50MB)
            CHECK_INTERVAL = 5  # Check every 5 chunks (5MB)
        elif part_count > 1000:  # Large files (>1GB)
            CHECK_INTERVAL = 20  # Check every 20 chunks (20MB) - less overhead
        else:  # Medium files (50MB-1GB)
            CHECK_INTERVAL = 10  # Default: every 10 chunks (10MB)
        
        # Enhancement 3: Pin this stream's chunks in cache to prevent eviction during playback
        GLOBAL_CACHE.pin_stream(stream_id)
        LOGGER.debug(f"[{stream_id[:8]}]: Stream pinned in cache")
        
        LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Producer starting. part_count={part_count} parallel={max_parallel}  (sessions={len(session_pool)}) check_interval={CHECK_INTERVAL}")

        # Schedule initial batch
        for i in range(min(part_count, max_parallel)):
            seq = next_to_schedule
            off = offset + seq * chunk_size
            LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Scheduling initial chunk seq={seq}")
            task = asyncio.create_task(fetch_chunk(seq, off))
            scheduled_tasks[seq] = task
            next_to_schedule += 1
        
        #  Main fetch loop
        while next_to_put < part_count:
            if stop_event.is_set():
                LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Producer stop_event detected in loop")
                break
            
            # Enhancement 12: Periodic ref count check - stop if no consumers
            if next_to_put % CHECK_INTERVAL == 0 and next_to_put > 0:
                async with PIPELINE_LOCK:
                    if stream_id in STREAM_PIPELINES:
                        pipeline_ref = STREAM_PIPELINES[stream_id]
                        if pipeline_ref.ref_count <= 0:
                            LOGGER.info(
                                f"Stream {stream_id[:8]}: No active consumers "
                                f"(ref_count=0 at chunk {next_to_put}/{part_count}), stopping producer"
                            )
                            pipeline.error = Exception("All consumers disconnected")
                            await q.put((None, None))
                            return
                    else:
                        # Pipeline was removed externally
                        LOGGER.warning(f"Stream {stream_id[:8]}: Pipeline removed externally, stopping")
                        return
            
            if not scheduled_tasks:
                LOGGER.debug(f"DEBUG [{stream_id[:8]}]: No scheduled tasks remaining")
                break
            
            # Enhancement 6: Watchdog timeout - detect stuck tasks
            try:
                done, pending = await asyncio.wait(
                    scheduled_tasks.values(), 
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=WATCHDOG_TIMEOUT
                )
                
                if not done:
                    # Timeout - log stuck tasks and abort
                    LOGGER.error(f"Stream {stream_id[:8]}: Producer watchdog timeout! {len(pending)} tasks stuck")
                    for seq, task in scheduled_tasks.items():
                        if not task.done():
                            LOGGER.error(f"  Stuck chunk: seq={seq}")
                            task.cancel()
                    pipeline.error = Exception("Producer watchdog timeout")
                    await q.put((None, None))
                    return
            except asyncio.TimeoutError:
                LOGGER.error(f"Stream {stream_id[:8]}: Producer timeout for stream")
                pipeline.error = Exception("Producer timeout")
                await q.put((None, None))
                return
            
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
                LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Task for seq={seq_idx} completed")
                
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
                    LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Scheduling next seq={seq}")
                    task = asyncio.create_task(fetch_chunk(seq, off))
                    scheduled_tasks[seq] = task
                    next_to_schedule += 1
            
            # Put sequential chunks in queue
            while next_to_put in results_buffer:
                chunk_bytes = results_buffer.pop(next_to_put)
                LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Putting seq={next_to_put} to queue (size={len(chunk_bytes)})")
                await q.put((offset + next_to_put * chunk_size, chunk_bytes))
                next_to_put += 1
        
        LOGGER.debug(f"DEBUG [{stream_id[:8]}]: Producer finished all chunks. Sending None.")
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
    finally:
        # Enhancement 3: Unpin stream from cache when producer stops
        GLOBAL_CACHE.unpin_stream(stream_id)
        LOGGER.debug(f"[{stream_id[:8]}]: Stream unpinned from cache")


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
    LOGGER.debug(f"DEBUG: Stream {stream_id[:8]}: Consumer started. Request={request is not None}")
    
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
                LOGGER.debug(f"DEBUG: Stream {stream_id[:8]}: Consumer received EOF sentinel")
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
        if not hasattr(self.client, "media_sessions"):
            self.client.media_sessions = {}
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
                    # If this is the bot's home DC, use the client itself
                    self.client.media_sessions[dc] = self.client
                    LOGGER.debug(f"Bot{self.client.name if hasattr(self.client, 'name') else '?'} - Pre-warmed DC {dc} (Home DC)")
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
                LOGGER.debug(f"Bot{self.client.name if hasattr(self.client, 'name') else '?'} - Pre-warmed DC {dc}")
                
            except Exception as e:
                LOGGER.warning(f"Could not pre-warm DC {dc}: {e}")
                continue

    async def get_file_properties(self, chat_id: int, message_id: int, force_refresh: bool = False) -> FileId:
        if force_refresh or message_id not in self._file_id_cache:
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
        parallelism: int = 3,  # Increased from 2 to utilize 3-bot pipeline effectively
        request: Optional[Request] = None,
        additional_client_indices: List[int] = [],
        chat_id: int = 0,
        message_id: int = 0,
    ):
        """
        Streaming with Global Pipeline Manager.
        
        Same stream_id → Same pipeline → Same bots → Same queue
        """
        if not stream_id:
            stream_id = secrets.token_hex(8)

        LOGGER.debug(f"DEBUG: prefetch_stream called: stream_id={stream_id} offset={offset} clients={len(additional_client_indices)+1}")
        
        pipeline = None
        is_pipeline_creator = False
        
        # Force a unique pipeline ID for every request to avoid queue sharing (race conditions)
        # But keep the cache_key (based on stream_id) same for cache hits.
        pipeline_id = f"{stream_id}_{secrets.token_hex(4)}"

        LOGGER.debug(f"DEBUG: prefetch_stream called: stream_id={stream_id} pipeline_id={pipeline_id} offset={offset} clients={len(additional_client_indices)+1}")
        
        pipeline = None
        is_pipeline_creator = False
        
        try:
            # === PHASE 1: Create Pipeline (Always New) ===
            async with PIPELINE_LOCK:
                # We do NOT check STREAM_PIPELINES for reuse anymore to prevent
                # multiple consumers stealing chunks from the same queue.
                
                # Create new pipeline
                is_pipeline_creator = True
                
                pipeline = StreamPipeline(
                    client_index,
                    additional_client_indices,
                    offset,
                    queue_size=max(prefetch, 20)
                )
                pipeline.ref_count = 1
                
                # Register in ACTIVE_STREAMS (for stats)
                # Use pipeline_id so dashboard shows all active connections
                now = time.time()
                ACTIVE_STREAMS[pipeline_id] = {
                    "stream_id": stream_id, # Original ID for reference
                    "pipeline_id": pipeline_id,
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
                
                STREAM_PIPELINES[pipeline_id] = pipeline
                LOGGER.info(f"Stream {stream_id[:8]} (Pipe {pipeline_id}): NEW pipeline (bots={pipeline.all_client_indices()}, offset={offset})")
        
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
                        offset, part_count, chunk_size, parallelism, stream_id,
                        byte_streamer=self, 
                        chat_id=chat_id, 
                        message_id=message_id
                    )
                )
            
            # === PHASE 3: Consume (All Requests) ===
            async for chunk in _consumer_generator(
                pipeline, request, first_part_cut, last_part_cut,
                part_count, pipeline_id, is_pipeline_creator
            ):
                yield chunk
        
        finally:
            # === PHASE 4: Cleanup ===
            if pipeline:
                async with PIPELINE_LOCK:
                    pipeline.ref_count -= 1
                    LOGGER.debug(f"Stream {stream_id[:8]} (Pipe {pipeline_id}): Exit (ref={pipeline.ref_count})")
                    
                    if pipeline.ref_count <= 0:
                        # Enhancement 2: Prevent race condition - check if cleanup already scheduled
                        if pipeline.delayed_cleanup_task and not pipeline.delayed_cleanup_task.done():
                            LOGGER.debug(f"Stream {stream_id[:8]}: Cleanup already scheduled, skipping")
                        else:
                            try:
                                from Backend.config import Telegram
                                cleanup_delay = getattr(Telegram, 'STREAM_CLEANUP_DELAY', 30)
                            except:
                                cleanup_delay = 30
                            LOGGER.info(f"Stream {stream_id[:8]} (Pipe {pipeline_id}): Starting delayed cleanup ({cleanup_delay}s)")
                            pipeline.delayed_cleanup_task = asyncio.create_task(self._delayed_cleanup(pipeline_id, pipeline))

    async def _delayed_cleanup(self, stream_id: str, pipeline: StreamPipeline):
        # Enhancement 14: Configurable cleanup delay
        try:
            from Backend.config import Telegram
            cleanup_delay = getattr(Telegram, 'STREAM_CLEANUP_DELAY', 10)
        except:
            cleanup_delay = 10
        
        LOGGER.debug(f"DEBUG: Stream {stream_id[:8]}: Delayed cleanup started (wait={cleanup_delay}s). Current ref_count={pipeline.ref_count}")
        try:
            await asyncio.sleep(cleanup_delay)
            
            async with PIPELINE_LOCK:
                LOGGER.debug(f"DEBUG: Stream {stream_id[:8]}: Delayed cleanup woke up. ref_count={pipeline.ref_count}")
                # Check if still valid for cleanup (might have been reused during sleep)
                if pipeline.ref_count <= 0:
                    LOGGER.info(f"Stream {stream_id[:8]}: Performing final cleanup")
                    
                    pipeline.stop_event.set()
                    
                    if pipeline.producer_task and not pipeline.producer_task.done():
                        pipeline.producer_task.cancel()
                        try:
                            await asyncio.wait_for(pipeline.producer_task, timeout=2.0)
                        except:
                            pass
                    
                    # Decrement workloads (last consumer cleans up)
                    try:
                        if work_loads[pipeline.client_index] > 0:
                            work_loads[pipeline.client_index] -= 1
                        for idx in pipeline.additional_client_indices:
                            if idx in work_loads and work_loads[idx] > 0:
                                work_loads[idx] -= 1
                    except:
                        pass
                    
                    # Enhancement 11: Stream lifecycle logging with metrics
                    # Move to RECENT_STREAMS with enhanced tracking
                    if stream_id in ACTIVE_STREAMS:
                        try:
                            entry = ACTIVE_STREAMS[stream_id]
                            end_ts = time.time()
                            duration = end_ts - entry["start_ts"]
                            entry.update({
                                "end_ts": end_ts,
                                "duration": duration,
                                "status": entry.get("status", "finished"),
                            })
                            RECENT_STREAMS.appendleft(ACTIVE_STREAMS.pop(stream_id))
                            
                            # Log lifecycle summary
                            LOGGER.info(
                                f"Stream {stream_id[:8]} LIFECYCLE: "
                                f"duration={duration:.1f}s, "
                                f"file={entry.get('file_name', '?')[:30]}, "
                                f"bots={len(entry.get('bots', []))}"
                            )
                        except Exception as log_err:
                            LOGGER.debug(f"Error logging lifecycle: {log_err}")
                    
                    # Enhancement 10: Log cache metrics on cleanup
                    try:
                        cache_stats = GLOBAL_CACHE.get_stats()
                        LOGGER.info(
                            f"Cache metrics: {cache_stats['hit_rate']} hit rate, "
                            f"{cache_stats['size_mb']:.0f}MB/{GLOBAL_CACHE.max_size / (1024*1024):.0f}MB used, "
                            f"{cache_stats['items']} items, "
                            f"{cache_stats['evictions']} evictions, "
                            f"{cache_stats['pinned_streams']} pinned streams"
                        )
                    except Exception as cache_err:
                        LOGGER.debug(f"Error getting cache stats: {cache_err}")
                    
                    STREAM_PIPELINES.pop(stream_id, None)
                else:
                     LOGGER.info(f"Stream {stream_id[:8]}: Cleanup aborted (ref_count={pipeline.ref_count})")
        
        except asyncio.CancelledError:
             LOGGER.debug(f"Stream {stream_id[:8]}: Delayed cleanup cancelled (resurrected)")
        except Exception as e:
             LOGGER.error(f"Stream {stream_id[:8]}: Error in delayed cleanup: {e}")
        finally:
             if pipeline.delayed_cleanup_task == asyncio.current_task():
                  pipeline.delayed_cleanup_task = None


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
