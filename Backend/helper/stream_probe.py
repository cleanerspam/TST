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
        "pseudo_1080p": re.compile(r"1080p?", re.IGNORECASE),
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
            "is_1080p": bool(StreamProbe.PATTERNS["pseudo_1080p"].search(filename)),
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
                LOGGER.warning(f"Probe failed: {stderr.decode()}")
                return {"error": "ffprobe_failed"}

            data = json.loads(stdout.decode())
            return StreamProbe._normalize_probe_data(data)

        except Exception as e:
            LOGGER.error(f"Probe Exception: {e}")
            return {"error": str(e)}

    @staticmethod
    def _normalize_probe_data(raw_data: dict) -> Dict[str, Any]:
        """Converts raw FFprobe JSON into our internal Standardized Metadata format."""
        streams = raw_data.get("streams", [])
        fmt = raw_data.get("format", {})
        
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
    @staticmethod
    async def parallel_probe(files: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Probes a list of files in parallel using available bots.
        Args:
            files: List of dicts, each containing {"id": "unique_id", "link": "http...", "filename": "...", "tg_file_ref": {...}}
            * tg_file_ref: Optional dict with {"client": client_obj, "chat_id": int, "msg_id": int}
        """
        results = {}
        semaphore = asyncio.Semaphore(10) # Max 10 concurrent probes

        async def _worker(file_info):
            async with semaphore:
                # 1. Semantic Parse (Always runs)
                semantic = StreamProbe.semantic_parse(file_info["filename"])
                
                # 2. Live Probe
                probe_data = {}
                error = None
                
                try:
                    # A. Telegram Partial Download (Preferred)
                    if "tg_file_ref" in file_info:
                        ref = file_info["tg_file_ref"]
                        probe_data = await StreamProbe.probe_telegram_file(
                            ref["client"], ref["chat_id"], ref["msg_id"]
                        )
                        
                    # B. Direct Link (Fallback/Alternative)
                    elif "link" in file_info and file_info["link"]:
                         probe_data = await StreamProbe.probe_file(file_info["link"])
                         
                except Exception as e:
                    error = str(e)
                    LOGGER.error(f"Probe failed for {file_info.get('id')}: {e}")

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
    # 4. Telegram Partial Download
    # -------------------------------------------------------------------------
    # -------------------------------------------------------------------------
    # 4. Telegram Partial Download (ByteStreamer Pipeline)
    # -------------------------------------------------------------------------
    @staticmethod
    async def probe_telegram_file(client, chat_id: int, msg_id: int) -> Dict[str, Any]:
        """
        Downloads the first 20MB of a Telegram file to a temp file using ByteStreamer.
        This uses the robust streaming pipeline (parallel bots, caching, retries).
        """
        import os
        import time
        import secrets
        from Backend.helper.custom_dl import ByteStreamer
        from Backend.pyrofork.bot import multi_clients
        from Backend.config import Telegram
        
        temp_file = f"/tmp/probe_{chat_id}_{msg_id}_{int(time.time())}.mkv"
        stream_id = f"probe_{secrets.token_hex(6)}"
        
        try:
            # 1. Initialize ByteStreamer
            streamer = ByteStreamer(client)
            
            # 2. Get File Properties
            file_id = await streamer.get_file_properties(chat_id, msg_id)
            
            # 3. Determine Client Index
            client_index = next((k for k, v in multi_clients.items() if v == client), 0)
            
            # 4. Configure Download (First 20MB)
            chunk_size = 1024 * 1024 # 1MB
            limit_mb = 20
            part_count = limit_mb # 20 chunks
            
            # Adjust dependencies if file is smaller than 20MB
            if file_id.file_size < (limit_mb * chunk_size):
                 part_count = (file_id.file_size // chunk_size) + 1
            
            # 5. Start Pipeline
            # We use prefetch_stream which returns an async generator
            body_gen = streamer.prefetch_stream(
                file_id=file_id,
                client_index=client_index,
                offset=0,
                first_part_cut=0,
                last_part_cut=chunk_size, # Take full chunk
                part_count=part_count,
                chunk_size=chunk_size,
                prefetch=3,
                stream_id=stream_id,
                meta={"type": "probe"},
                parallelism=2,
                additional_client_indices=[] # Simple probe, no helpers needed usually
            )
            
            # 6. Consume Stream to File
            downloaded = 0
            with open(temp_file, "wb") as f:
                async for chunk in body_gen:
                     if chunk:
                         f.write(chunk)
                         downloaded += len(chunk)
            
            if downloaded == 0:
                return {"error": "download_failed_empty"}
                
            # 7. Probe Temp File
            return await StreamProbe.probe_file(temp_file)
            
        except Exception as e:
            LOGGER.error(f"TG Probe Error ({stream_id}): {e}")
            return {"error": str(e)}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
