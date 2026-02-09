import asyncio
import re
import math
import subprocess
import json
import logging
from typing import Dict, Any, List, Optional
from Backend.config import Telegram
from Backend.helper.task_manager import get_next_client, multi_clients
from Backend.logger import LOGGER

# Module-level counters for tracking probe statistics
# These will be reset for each batch of files
probe_success_count = 0
probe_failure_count = 0
current_batch_total = 0  # Track the total files in the current batch

class StreamProbe:
    """
    Handles deep inspection of media files via:
    1. Semantic Filename Parsing (Fast, Fallback)
    2. FFprobe Header Analysis (Slow, Accurate)
    """
    
    # -------------------------------------------------------------------------
    # 1. Semantic Parsing (Regex Magic)
    # -------------------------------------------------------------------------
    PATTERNS = {
        "dual_audio": re.compile(r"\b(dual|multi)[ ._-]?audio\b|\bstart[ ._-]?audio\b", re.IGNORECASE),
        "esub": re.compile(r"\b(esubs?|e[ ._-]?subs?)\b|\beng(lish)?[ ._-]?sub(s|title)?\b", re.IGNORECASE),
        "hsub": re.compile(r"\b(hsub|h[ ._-]?sub)\b|\bhin(di)?[ ._-]?sub(s|title)?\b", re.IGNORECASE),
        "res_8k": re.compile(r"(8k|4320p?)", re.IGNORECASE),
        "res_4k": re.compile(r"(4k|2160p?)", re.IGNORECASE),
        "res_1080p": re.compile(r"1080p?", re.IGNORECASE),
        "res_720p": re.compile(r"720p?", re.IGNORECASE),
        "res_576p": re.compile(r"576p?", re.IGNORECASE),
        "res_480p": re.compile(r"480p?", re.IGNORECASE),
        "res_360p": re.compile(r"360p?", re.IGNORECASE),
        "res_240p": re.compile(r"240p?", re.IGNORECASE),
        "hevc": re.compile(r"(hevc|x265|h\.?265)", re.IGNORECASE),
        "10bit": re.compile(r"(10[ ._-]?bit|hi10p)", re.IGNORECASE),
        "aac": re.compile(r"\baac\b", re.IGNORECASE),
        "eac3": re.compile(r"(eac3|dd\+|dolby[ ._-]?digital[ ._-]?plus)", re.IGNORECASE),
        "truehd": re.compile(r"(truehd|atmos)", re.IGNORECASE),
        "channels_6": re.compile(r"(6ch|5\.1)", re.IGNORECASE),
    }

    @staticmethod
    def semantic_parse(filename: str) -> Dict[str, Any]:
        """Extracts technical metadata purely from filename tags."""
        tags = {
            "is_dual_audio": bool(StreamProbe.PATTERNS["dual_audio"].search(filename)),
            "has_esub": bool(StreamProbe.PATTERNS["esub"].search(filename)),
            "has_hsub": bool(StreamProbe.PATTERNS["hsub"].search(filename)),
            "is_hevc": bool(StreamProbe.PATTERNS["hevc"].search(filename)),
            "is_10bit": bool(StreamProbe.PATTERNS["10bit"].search(filename)),
            
            # Resolution Tags
            "is_4320p": bool(StreamProbe.PATTERNS["res_8k"].search(filename)),
            "is_2160p": bool(StreamProbe.PATTERNS["res_4k"].search(filename)),
            "is_1080p": bool(StreamProbe.PATTERNS["res_1080p"].search(filename)),
            "is_720p": bool(StreamProbe.PATTERNS["res_720p"].search(filename)),
            "is_576p": bool(StreamProbe.PATTERNS["res_576p"].search(filename)),
            "is_480p": bool(StreamProbe.PATTERNS["res_480p"].search(filename)),
            "is_360p": bool(StreamProbe.PATTERNS["res_360p"].search(filename)),
            "is_240p": bool(StreamProbe.PATTERNS["res_240p"].search(filename)),

            "has_aac": bool(StreamProbe.PATTERNS["aac"].search(filename)),
            "has_eac3": bool(StreamProbe.PATTERNS["eac3"].search(filename)),
            "has_truehd": bool(StreamProbe.PATTERNS["truehd"].search(filename)),
            "is_surround": bool(StreamProbe.PATTERNS["channels_6"].search(filename)),
        }
        
        # Inferred logic
        if tags["has_hsub"] and tags["has_esub"]:
             tags["sub_combo"] = "hin_eng"
        elif tags["has_esub"]:
             tags["sub_combo"] = "eng"
        else:
             tags["sub_combo"] = "none"

        return tags

    # -------------------------------------------------------------------------
    # 2. FFprobe Deep Inspection
    # -------------------------------------------------------------------------
    @staticmethod
    async def probe_file(file_url: str) -> Dict[str, Any]:
        """
        Streams the first 10-20MB of a file to ffprobe to extract reliable metadata.
        Returns a structured dictionary of technical details.
        """
        headers = {"User-Agent": "TelegramBot (like TwitterBot)"} # Sometimes needed
        
        # Construct FFprobe command
        # -v error: Silence output
        # -show_streams -show_format: Get data
        # -print_format json: Easy parsing
        # -analyzeduration 20M: Limit analysis time
        # -probesize 20M: Limit analysis buffer
        
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_streams",
            "-show_format",
            "-print_format", "json",
            "-analyzeduration", "50000000", # 50s equivalent (for header)
            "-probesize", "50000000",      # 50MB buffer (safe for 4K)
            file_url
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait with a timeout to prevent hanging on bad connections
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
            except asyncio.TimeoutError:
                process.kill()
                LOGGER.warning(f"Probe timed out for {file_url[:30]}...")
                return {"error": "timeout"}

            if process.returncode != 0:
                err_msg = stderr.decode()
                # Quietly log generic errors, but warn on specific ones if needed
                # LOGGER.warning(f"Probe failed: {err_msg}") 
                return {"error": "ffprobe_failed", "details": err_msg}

            data = json.loads(stdout.decode())
            return StreamProbe._normalize_probe_data(data, file_url)

        except Exception as e:
            LOGGER.error(f"Probe Exception for {file_url[:50]}: {e}")
            return {"error": str(e)}

    @staticmethod
    def _normalize_probe_data(raw_data: dict, source_id: str = "unknown") -> Dict[str, Any]:
        """Converts raw FFprobe JSON into our internal Standardized Metadata format."""
        streams = raw_data.get("streams", [])
        fmt = raw_data.get("format", {})
        
        # Validation: Check if we actually got video streams
        has_video = any(s.get("codec_type") == "video" for s in streams)
        if not has_video:
             LOGGER.error(f"PROBE ERROR: No video stream found for {source_id[-20:]}. Raw: {str(raw_data)[:200]}")
        
        normalized = {
            "duration": float(fmt.get("duration", 0)),
            "size": int(fmt.get("size", 0)),
            "bitrate": int(fmt.get("bit_rate", 0)),
            "container": fmt.get("format_name", "").split(",")[0],
            "video": {},
            "audio": [],
            "subtitle": []
        }

        for s in streams:
            stype = s.get("codec_type")
            
            if stype == "video":
                # Only take the first video stream (usually the main one)
                if not normalized["video"]:
                    normalized["video"] = {
                        "codec": s.get("codec_name"),
                        "width": int(s.get("width", 0)),
                        "height": int(s.get("height", 0)),
                        "profile": s.get("profile"),
                        "pix_fmt": s.get("pix_fmt"),
                        "level": s.get("level"),
                        "is_hdr": "hdr" in str(s.get("color_transfer", "")).lower() or "smpte2084" in str(s.get("color_transfer", "")).lower()
                    }
                    
                    # Detect 10-bit
                    pix = s.get("pix_fmt", "")
                    if "10" in pix or "p10" in pix:
                        normalized["video"]["depth"] = 10
                    else:
                        normalized["video"]["depth"] = 8

            elif stype == "audio":
                lang = s.get("tags", {}).get("language", "und")
                normalized["audio"].append({
                    "codec": s.get("codec_name"),
                    "channels": int(s.get("channels", 2)),
                    "lang": lang,
                    "title": s.get("tags", {}).get("title", "")
                })

            elif stype == "subtitle":
                lang = s.get("tags", {}).get("language", "und")
                normalized["subtitle"].append({
                    "codec": s.get("codec_name"),
                    "lang": lang,
                    "is_sdh": "sdh" in str(s.get("tags", {}).get("title", "")).lower()
                })

        return normalized

    # -------------------------------------------------------------------------
    # 3. Parallel Execution Logic
    # -------------------------------------------------------------------------
    # -------------------------------------------------------------------------
    # 3. Parallel Execution Logic
    # -------------------------------------------------------------------------
    _bot_locks: Dict[int, asyncio.Lock] = {}

    @classmethod
    def get_bot_lock(cls, bot_id: int) -> asyncio.Lock:
        if bot_id not in cls._bot_locks:
            cls._bot_locks[bot_id] = asyncio.Lock()
        return cls._bot_locks[bot_id]

    @staticmethod
    async def parallel_probe(files: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Probes a list of files in parallel using available bots.
        Safely manages concurrency to prevent FloodWait:
        - Global Limit: len(multi_clients) (One probe per bot)
        - Per-Bot Limit: 1 (Strictly one connection per account)
        """
        results = {}
        # Dynamic semaphore based on available bots (min 1, max 20)
        num_bots = max(1, len(multi_clients))
        # Allow slightly more global parallelism than bots to keep pipeline full, 
        # but locks will ensure strict 1-per-bot execution.
        global_sem = asyncio.Semaphore(num_bots + 2) 

        async def _worker(file_info):
            async with global_sem:
                # 1. Semantic Parse (Always runs)
                semantic = StreamProbe.semantic_parse(file_info["filename"])
                
                # 2. Live Probe
                probe_data = {}
                error = None
                is_deep_probed = False
                
                # Safety Check: Warn if missing download context
                if "tg_file_ref" not in file_info and "link" not in file_info:
                    LOGGER.debug(f"PROBE SKIP: Missing download context for [{file_info.get('id')}].")
                
                try:
                    # A. Telegram Partial Download (Preferred)
                    if "tg_file_ref" in file_info:
                        ref = file_info["tg_file_ref"]
                        client = ref["client"]
                        # Get the Bot ID (or index) to lock on
                        # We use the index passed in 'client_index' if available, else derive from client ID
                        bot_idx = ref.get("client_index")
                        if bot_idx is None:
                            # Fallback if index not provided (rare)
                            bot_idx = id(client)

                        # Acquire Lock for THIS SPECIFIC BOT
                        # This ensures this bot is doing absolutely nothing else while probing this file
                        async with StreamProbe.get_bot_lock(bot_idx):
                            # LOGGER.debug(f"Acquired lock for Bot {bot_idx}, probing {file_info.get('id')}")
                            probe_data = await StreamProbe.probe_telegram_file(
                                client, ref["chat_id"], ref["msg_id"]
                            )
                        
                    # B. Direct Link (Fallback/Alternative)
                    elif "link" in file_info and file_info["link"]:
                         probe_data = await StreamProbe.probe_file(file_info["link"])
                         
                    # Check if we actually got tech metadata
                    if probe_data and (probe_data.get("video") or probe_data.get("audio")):
                        is_deep_probed = True
                         
                except Exception as e:
                    error = str(e)
                    LOGGER.debug(f"Probe failed for {file_info.get('id')}: {e}")

                # 3. Merge
                if "error" in probe_data:
                    error = probe_data["error"]
                    probe_data = {} # Reset if error
                    is_deep_probed = False

                results[file_info["id"]] = {
                    "filename": file_info["filename"],
                    "semantic": semantic,
                    "probe": probe_data,
                    "error": error,
                    "is_deep_probed": is_deep_probed,
                    "probed_at": datetime.utcnow()
                }

        tasks = [_worker(f) for f in files]
        await asyncio.gather(*tasks)
        return results

    # -------------------------------------------------------------------------
    # 4. Telegram Partial Download (ByteStreamer Pipeline)
    # -------------------------------------------------------------------------
    @staticmethod
    async def probe_telegram_file(initial_client, chat_id: int, msg_id: int) -> Dict[str, Any]:
        """
        Downloads the first 5MB of a Telegram file to a temp file using ByteStreamer.
        Strictly uses the provided client to respect bot locks.
        """
        global probe_success_count, probe_failure_count, current_batch_total

        import os
        import time
        import secrets
        import tempfile
        from Backend.helper.custom_dl import ByteStreamer
        from Backend.pyrofork.bot import multi_clients
        
        # Use cross-platform temp directory
        temp_dir = tempfile.gettempdir()
        temp_filename = f"probe_{chat_id}_{msg_id}_{int(time.time())}.mkv"
        temp_file = os.path.join(temp_dir, temp_filename)
        
        stream_id_base = f"probe_{secrets.token_hex(6)}"
        last_error = None

        # We ONLY use the initial_client passed to us. 
        # The parallel_probe logic handles load balancing. 
        # We don't want this function to "go rogue" and switch to another bot 
        # that might be locked by another thread.
        client = initial_client

        try:
            # --- Worker Function (Same as before, just inside loop) ---
            async def download_range(streamer, file_id, client_index, offset, part_count, sid):
                """Download a specific range of the file."""
                # Increased buffer to 1MB chunks
                chunk_size = 1024 * 1024 
                data = bytearray()
                
                body_gen = streamer.prefetch_stream(
                    file_id=file_id,
                    client_index=client_index,
                    offset=offset,
                    first_part_cut=0,
                    last_part_cut=chunk_size,
                    part_count=part_count,
                    chunk_size=chunk_size,
                    prefetch=1,
                    stream_id=sid,
                    meta={"type": "probe"},
                    parallelism=1,
                    additional_client_indices=[] # STRICT: No helpers
                )
                
                try:
                    # Increased timeout to 30s as requested
                    async with asyncio.timeout(30):
                        async for chunk in body_gen:
                            if chunk:
                                data.extend(chunk)
                except asyncio.TimeoutError:
                    LOGGER.warning(f"Probe Stream Timeout using client {client_index}")
                    return None
                except Exception as e:
                    LOGGER.warning(f"Probe Stream Error: {e}")
                    return None
                
                return bytes(data)

            # 1. Initialize ByteStreamer
            streamer = ByteStreamer(client)
            
            # 2. Get File Properties
            try:
                file_id = await asyncio.wait_for(
                    streamer.get_file_properties(chat_id, msg_id),
                    timeout=10.0
                )
            except Exception as e:
                raise e
            
            # 3. Determine Client Index
            client_index = next((k for k, v in multi_clients.items() if v == client), 0)
            
            chunk_size = 1024 * 1024
            file_size = file_id.file_size
            stream_id = f"{stream_id_base}_direct"

            # 4. Smart Download Strategy
            # Analyze first 5MB (Increased from 2MB)
            PROBE_SIZE_MB = 5
            parts_to_dl = PROBE_SIZE_MB
            
            if file_size < PROBE_SIZE_MB * chunk_size:
                # Small file: Download all
                part_count = max(1, (file_size // chunk_size) + 1)
                first_data = await download_range(streamer, file_id, client_index, 0, part_count, stream_id)

                if not first_data:
                    raise Exception("Empty Data (Small File)")

                probe_success_count += 1
                LOGGER.info(f"FFPROBE SUCC: Small file (Bot {client_index}) - {len(first_data)} bytes [{probe_success_count}/{current_batch_total}]")

                with open(temp_file, "wb") as f:
                    f.write(first_data)
                return await StreamProbe.probe_file(temp_file)
            
            # Large File: Download first 5MB
            first_data = await download_range(streamer, file_id, client_index, 0, parts_to_dl, f"{stream_id}_start")
            if not first_data:
                raise Exception("Empty Data (Header)")

            # Write temp file safely
            with open(temp_file, "wb") as f:
                f.write(first_data)
            
            # Probe Header
            result = await StreamProbe.probe_file(temp_file)
            if result.get("video") or result.get("audio"):
                probe_success_count += 1
                LOGGER.info(f"FFPROBE SUCC: Header analysis (Bot {client_index}) [{probe_success_count}/{current_batch_total}]")
                return result
                
            # If header failed, try last 2 chunks (MP4 moov atom at end?)
            # This is a fallback attempt
            last_offset = max(0, (file_size // chunk_size) - 2)
            last_data = await download_range(streamer, file_id, client_index, last_offset * chunk_size, 2, f"{stream_id}_end")

            if last_data:
                LOGGER.info(f"FFPROBE: Fetching trailer for MOOV atom (Bot {client_index})...")
                with open(temp_file, "ab") as f: # Append
                    f.write(last_data)
                result = await StreamProbe.probe_file(temp_file)
                if result.get("video") or result.get("audio"):
                    probe_success_count += 1
                    LOGGER.info(f"FFPROBE SUCC: Header+Trailer (Bot {client_index}) [{probe_success_count}/{current_batch_total}]")
                else:
                    LOGGER.info(f"FFPROBE FAIL: Even with trailer (Bot {client_index})")
                return result

            return result # Return whatever we got from header

        except Exception as e:
            if "Empty Data" in str(e):
                probe_failure_count += 1
                LOGGER.warning(f"FFPROBE FAIL: {e}")
            last_error = e
            return {"error": str(e)}

        finally:
            # STRICT CLEANUP
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as e:
                    LOGGER.error(f"Failed to remove temp file {temp_file}: {e}")


def get_probe_statistics():
    """
    Returns the current probe statistics
    """
    return {
        "successful_probes": probe_success_count,
        "failed_probes": probe_failure_count,
        "total_probes": probe_success_count + probe_failure_count,
        "batch_total": current_batch_total,
        "success_rate": (probe_success_count / (probe_success_count + probe_failure_count) * 100) if (probe_success_count + probe_failure_count) > 0 else 0,
        "completion_rate": ((probe_success_count + probe_failure_count) / current_batch_total * 100) if current_batch_total > 0 else 0
    }


def reset_batch_counters(batch_total=0):
    """
    Resets the counters for a new batch of files
    """
    global probe_success_count, probe_failure_count, current_batch_total
    probe_success_count = 0
    probe_failure_count = 0
    current_batch_total = batch_total
    LOGGER.info(f"FFPROBE BATCH RESET: Starting new batch with {batch_total} files")


def log_probe_statistics():
    """
    Logs the current probe statistics
    """
    stats = get_probe_statistics()
    batch_info = f" (Batch Total: {current_batch_total})" if current_batch_total > 0 else ""
    LOGGER.info(f"FFPROBE STATISTICS: Total={stats['total_probes']}, Successful={stats['successful_probes']}, Failed={stats['failed_probes']}, Success Rate={stats['success_rate']:.2f}%{batch_info}")

