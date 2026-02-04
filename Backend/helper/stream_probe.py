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
        "esub": re.compile(r"\b(esub|e[ ._-]?sub)\b|\beng(lish)?[ ._-]?sub(s|title)?\b", re.IGNORECASE),
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
    @staticmethod
    async def parallel_probe(files: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Probes a list of files in parallel using available bots.
        Args:
            files: List of dicts, each containing {"id": "unique_id", "link": "http...", "filename": "...", "tg_file_ref": {...}}
            * tg_file_ref: Optional dict with {"client": client_obj, "chat_id": int, "msg_id": int}
        """
        results = {}
        # Reduced from 10 to 3 to prevent overwhelming Telegram API
        semaphore = asyncio.Semaphore(3)
        probe_count = [0]  # Mutable counter for delay logic

        async def _worker(file_info):
            async with semaphore:
                # Add small delay every 3 probes to prevent rate limiting
                probe_count[0] += 1
                if probe_count[0] > 1 and probe_count[0] % 3 == 0:
                    await asyncio.sleep(0.5)
                
                # 1. Semantic Parse (Always runs)
                semantic = StreamProbe.semantic_parse(file_info["filename"])
                
                # 2. Live Probe
                probe_data = {}
                error = None
                
                # Safety Check: Warn if missing download context
                if "tg_file_ref" not in file_info and "link" not in file_info:
                    LOGGER.warning(f"PROBE SKIP: Missing download context for [{file_info.get('id')}]. Will use semantic fallback only.")
                
                try:
                    # A. Telegram Partial Download (Preferred)
                    if "tg_file_ref" in file_info:
                        ref = file_info["tg_file_ref"]
                        # Passing the original client for ref, but probe_telegram_file handles retry internally if needed
                        probe_data = await StreamProbe.probe_telegram_file(
                            ref["client"], ref["chat_id"], ref["msg_id"]
                        )
                        
                    # B. Direct Link (Fallback/Alternative)
                    elif "link" in file_info and file_info["link"]:
                         probe_data = await StreamProbe.probe_file(file_info["link"])
                         
                except Exception as e:
                    error = str(e)
                    # Log at debug level for probes (they fail often for deleted files)
                    LOGGER.debug(f"Probe failed for {file_info.get('id')}: {e}")

                # 3. Merge
                if "error" in probe_data:
                    error = probe_data["error"]
                    probe_data = {} # Reset if error

                results[file_info["id"]] = {
                    "filename": file_info["filename"],
                    "semantic": semantic,
                    "probe": probe_data,
                    "error": error,
                    "probed_at": 1 # timestamp placement
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
        Includes RETRY LOGIC: If a client/bot fails, it tries the next available bot.
        """
        import os
        import time
        import secrets
        from Backend.helper.custom_dl import ByteStreamer
        from Backend.pyrofork.bot import multi_clients
        from Backend.config import Telegram
        
        # Gather all available clients
        # 1. Start with the client that was passed in (to respect the initial context)
        # 2. Add rest of the clients as fallback
        
        available_clients = [initial_client]
        for idx, client in multi_clients.items():
            if client != initial_client:
                available_clients.append(client)

        temp_file = f"/tmp/probe_{chat_id}_{msg_id}_{int(time.time())}.mkv"
        stream_id_base = f"probe_{secrets.token_hex(6)}"
        
        last_error = None

        for attempt, client in enumerate(available_clients):
            try:
                # LOGGER.info(f"Probing with Client #{attempt+1}...")
                
                # --- Worker Function (Same as before, just inside loop) ---
                async def download_range(streamer, file_id, client_index, offset, part_count, sid):
                    """Download a specific range of the file."""
                    chunk_size = 1024 * 1024  # 1MB
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
                        additional_client_indices=[]
                    )
                    
                    try:
                        async with asyncio.timeout(20):
                            async for chunk in body_gen:
                                if chunk:
                                    data.extend(chunk)
                    except asyncio.TimeoutError:
                        LOGGER.warning(f"Probe Stream Timeout using client {client_index}")
                        raise Exception("Timeout")
                    
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
                    # LOGGER.warning(f"Client {attempt} failed to get props: {e}")
                    raise e
                
                # 3. Determine Client Index
                client_index = next((k for k, v in multi_clients.items() if v == client), 0)
                
                chunk_size = 1024 * 1024
                file_size = file_id.file_size
                stream_id = f"{stream_id_base}_{attempt}"

                # 4. Smart Download Strategy
                if file_size < 4 * chunk_size:
                    part_count = max(1, (file_size // chunk_size) + 1)
                    first_data = await download_range(streamer, file_id, client_index, 0, part_count, stream_id)

                    if not first_data:
                        LOGGER.warning(f"Chunk 0 download failed for small file (chat_id={chat_id}, msg_id={msg_id}) using client {client_index} (attempt {attempt+1})")
                        raise Exception("Empty Data")

                    global probe_success_count, probe_failure_count, current_batch_total
                    probe_success_count += 1
                    completion_percentage = ((probe_success_count + probe_failure_count) / current_batch_total * 100) if current_batch_total > 0 else 0
                    LOGGER.info(f"FFPROBE SUCCESS: Chunk 0 downloaded for small file (chat_id={chat_id}, msg_id={msg_id}) using client {client_index} (attempt {attempt+1}) - {len(first_data)} bytes [Batch: {probe_success_count} successful, {probe_failure_count} failed, {completion_percentage:.1f}% complete]")

                    with open(temp_file, "wb") as f:
                        f.write(first_data)
                    return await StreamProbe.probe_file(temp_file)
                
                # Step 1: Download first 2MB
                first_data = await download_range(streamer, file_id, client_index, 0, 2, f"{stream_id}_start")
                if not first_data:
                    LOGGER.warning(f"Chunk 0 download failed for client {client_index} (attempt {attempt+1})")
                    raise Exception("Empty Data")

                global probe_success_count, probe_failure_count, current_batch_total
                probe_success_count += 1
                completion_percentage = ((probe_success_count + probe_failure_count) / current_batch_total * 100) if current_batch_total > 0 else 0
                LOGGER.info(f"FFPROBE SUCCESS: Chunk 0 downloaded for file (chat_id={chat_id}, msg_id={msg_id}) using client {client_index} (attempt {attempt+1}) - {len(first_data)} bytes [Batch: {probe_success_count} successful, {probe_failure_count} failed, {completion_percentage:.1f}% complete]")

                with open(temp_file, "wb") as f:
                    f.write(first_data)
                
                result = await StreamProbe.probe_file(temp_file)
                if result.get("video") or result.get("audio"):
                    global probe_success_count, probe_failure_count, current_batch_total
                    probe_success_count += 1
                    completion_percentage = ((probe_success_count + probe_failure_count) / current_batch_total * 100) if current_batch_total > 0 else 0
                    LOGGER.info(f"FFPROBE ANALYSIS SUCCESS: Media analysis completed for file (chat_id={chat_id}, msg_id={msg_id}) [Batch: {probe_success_count} successful, {probe_failure_count} failed, {completion_percentage:.1f}% complete]")
                    return result
                    
                # Step 2: Last chunk (for MP4)
                last_offset = max(0, (file_size // chunk_size) - 2)
                last_data = await download_range(streamer, file_id, client_index, last_offset * chunk_size, 2, f"{stream_id}_end")

                if last_data:
                    LOGGER.info(f"FFPROBE PARTIAL SUCCESS: First chunk succeeded but no media data, using last chunk for file (chat_id={chat_id}, msg_id={msg_id}) - additional {len(last_data)} bytes")
                    with open(temp_file, "wb") as f:
                        f.write(first_data)
                        f.write(last_data)
                    result = await StreamProbe.probe_file(temp_file)
                    if result.get("video") or result.get("audio"):
                        global probe_success_count, probe_failure_count, current_batch_total
                        probe_success_count += 1
                        completion_percentage = ((probe_success_count + probe_failure_count) / current_batch_total * 100) if current_batch_total > 0 else 0
                        LOGGER.info(f"FFPROBE ANALYSIS SUCCESS: Media analysis completed using both chunks for file (chat_id={chat_id}, msg_id={msg_id}) [Batch: {probe_success_count} successful, {probe_failure_count} failed, {completion_percentage:.1f}% complete]")
                    return result
                else:
                    LOGGER.warning(f"FFPROBE PARTIAL: First chunk succeeded but last chunk failed for file (chat_id={chat_id}, msg_id={msg_id})")

                return result # Return whatever we got

            except Exception as e:
                # Also increment failure counter for exceptions during probing
                if "Empty Data" in str(e) or "All bots failed" in str(e):
                    global probe_success_count, probe_failure_count, current_batch_total
                    probe_failure_count += 1
                    completion_percentage = ((probe_success_count + probe_failure_count) / current_batch_total * 100) if current_batch_total > 0 else 0
                    LOGGER.warning(f"FFPROBE EXCEPTION: Failed to download chunk 0 for file (chat_id={chat_id}, msg_id={msg_id}), error: {e} [Batch: {probe_success_count} successful, {probe_failure_count} failed, {completion_percentage:.1f}% complete]")

                # LOGGER.warning(f"Probe attempt {attempt+1} failed: {e}")
                last_error = e
                # Clean up and continue to next bot
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                continue

        # If loop finishes without return
        failed_count = len(available_clients)  # Number of bots that failed
        from Backend.logger import LOGGER
        global probe_success_count, probe_failure_count, current_batch_total
        probe_failure_count += 1
        completion_percentage = ((probe_success_count + probe_failure_count) / current_batch_total * 100) if current_batch_total > 0 else 0
        LOGGER.error(f"FFPROBE FAILED for file (chat_id={chat_id}, msg_id={msg_id}): All {failed_count} bots failed to get chunk 0. Last error: {last_error} [Batch: {probe_success_count} successful, {probe_failure_count} failed, {completion_percentage:.1f}% complete]")
        return {"error": f"All bots failed. Last error: {last_error}"}


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

