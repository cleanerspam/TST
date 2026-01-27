import math
import secrets
import mimetypes
import time
import asyncio
from typing import Tuple, List
from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse

from Backend import db
from Backend.config import Telegram
from Backend.helper.encrypt import decode_string
from Backend.helper.exceptions import InvalidHash
from Backend.helper.custom_dl import ByteStreamer, ACTIVE_STREAMS, RECENT_STREAMS
from Backend.pyrofork.bot import StreamBot, work_loads, multi_clients, client_dc_map
from Backend.fastapi.security.tokens import verify_token
from pyrogram.file_id import FileId

router = APIRouter(tags=["Streaming"])
class_cache = {}



def parse_range_header(range_header: str, file_size: int) -> Tuple[int, int]:
    if not range_header:
        # Default to full content
        return 0, file_size - 1

    try:
        if not range_header.startswith("bytes="):
             raise ValueError("Invalid Range header format")

        range_value = range_header.replace("bytes=", "")
        parts = range_value.split("-")
        
        if len(parts) != 2:
             raise ValueError("Invalid Range header format")

        start_str, end_str = parts
        
        start = int(start_str) if start_str else 0
        end = int(end_str) if end_str else file_size - 1
        
        # Validation
        if start < 0: raise ValueError("Start cannot be negative")
        if end >= file_size: end = file_size - 1 
        if start > end:
            # Satisfiable if range is invalid? Standard says 416, but we can clamp
            raise ValueError("Start > End")
            
    except (ValueError, IndexError) as e:
        raise HTTPException(
            status_code=416,
            detail="Requested Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    return start, end


def select_best_clients(target_dc: int, count: int = 3) -> List[int]:
    """
    Selects the best bot clients for the given target DC.
    Prioritizes clients in the same DC to avoid inter-DC latency.
    Returns a list of client IDs.
    """
    # 1. Try to find a client in the same DC
    matching_dc_clients = []
    for client_id, client_dc in client_dc_map.items():
        if client_dc == target_dc and client_id in multi_clients:
             matching_dc_clients.append(client_id)
    
    if matching_dc_clients:
        # Sort by workload (ascending)
        sorted_clients = sorted(matching_dc_clients, key=lambda cid: work_loads.get(cid, 0))
        return sorted_clients[:count]

    # 2. Fallback: Lowest workload overall
    if multi_clients:
        sorted_clients = sorted(multi_clients.keys(), key=lambda cid: work_loads.get(cid, 0))
        return sorted_clients[:count]
    
    return [0]


async def track_usage_from_stats(stream_id: str, token: str, token_data: dict):
    """
    Background task to track bandwidth usage without blocking the stream.
    """
    await asyncio.sleep(2) # Wait for stream to initialize
    
    limits = token_data.get("limits", {}) if token_data else {}
    usage = token_data.get("usage", {}) if token_data else {}
    
    daily_limit_gb = limits.get("daily_limit_gb")
    monthly_limit_gb = limits.get("monthly_limit_gb")
    
    initial_daily_bytes = usage.get("daily", {}).get("bytes", 0)
    initial_monthly_bytes = usage.get("monthly", {}).get("bytes", 0)
    
    last_tracked_bytes = 0
    update_interval = 10 
    
    try:
        while True:
            await asyncio.sleep(update_interval)
            
            # Check if stream is still active
            stream_info = ACTIVE_STREAMS.get(stream_id)
            
            # If not active, check recent (it might have just finished)
            if not stream_info:
                for rec in RECENT_STREAMS:
                    if rec.get("stream_id") == stream_id:
                        final_bytes = rec.get("total_bytes", 0)
                        delta = final_bytes - last_tracked_bytes
                        if delta > 0:
                             try:
                                await db.update_token_usage(token, delta)
                             except Exception:
                                pass
                        return
                return # Stream lost/expired
            
            # Stream is active
            current_bytes = stream_info.get("total_bytes", 0)
            delta = current_bytes - last_tracked_bytes
            
            if delta > 0:
                try:
                    await db.update_token_usage(token, delta)
                    last_tracked_bytes = current_bytes
                except Exception:
                    pass
            
            # Check limits (logging only, or aggressive cancellation could be added here)
            if daily_limit_gb and daily_limit_gb > 0:
                 current_daily_gb = (initial_daily_bytes + current_bytes) / (1024 ** 3)
                 if current_daily_gb >= daily_limit_gb:
                      # Could trigger stream cancellation here
                      pass

                                
    except asyncio.CancelledError:
        # Final update on cancellation
        stream_info = ACTIVE_STREAMS.get(stream_id)
        if stream_info:
            current_bytes = stream_info.get("total_bytes", 0)
            delta = current_bytes - last_tracked_bytes
            if delta > 0:
                 try: 
                    await db.update_token_usage(token, delta) 
                 except: 
                    pass



@router.get("/dl/{token}/{id}/{name}")
@router.head("/dl/{token}/{id}/{name}")
async def stream_handler(request: Request, token: str, id: str, name: str, token_data: dict = Depends(verify_token)):
    decoded_data = await decode_string(id)
    if not decoded_data.get("msg_id"):
        raise HTTPException(status_code=400, detail="Missing id")

    chat_id = f"-100{decoded_data['chat_id']}"
    message = await StreamBot.get_messages(int(chat_id), int(decoded_data["msg_id"]))
    file = message.video or message.document
    file_hash = file.file_unique_id[:6]
    
    try:
        file_id_obj = FileId.decode(file.file_id)
        dc_id = file_id_obj.dc_id
    except Exception:
        dc_id = None

    return await media_streamer(
        request,
        chat_id=int(chat_id),
        id=int(decoded_data["msg_id"]),
        secure_hash=file_hash,
        token_data=token_data,
        dc_id=dc_id
    )


async def media_streamer(
    request: Request,
    chat_id: int,
    id: int,
    secure_hash: str,
    token_data: dict = None,
    dc_id: int = None
) -> StreamingResponse:
    range_header = request.headers.get("Range", "")
    
    # If DC ID known, use it. Else fall back to 0 (StreamBot/random)
    target_dc = dc_id if dc_id else 0
    client_indices = select_best_clients(target_dc, count=3)
    client_idx = client_indices[0]
    additional_client_indices = client_indices[1:]
    
    # Debug: Log which bots were selected
    from Backend.logger import LOGGER
    LOGGER.info(f"Stream starting: Selected bots {client_indices} for DC{target_dc} (Primary: Bot{client_idx}, Helpers: {additional_client_indices})")
    
    faster_client = multi_clients[client_idx]
    
    # Ensure ByteStreamer exists for ALL selected clients (Primary + Helpers)
    # This triggers pre-warming for them
    for idx in client_indices:
        cli = multi_clients[idx]
        if cli not in class_cache:
            class_cache[cli] = ByteStreamer(cli)

    tg_connect = class_cache[faster_client]

    file_id = await tg_connect.get_file_properties(chat_id=chat_id, message_id=id)
    if file_id.unique_id[:6] != secure_hash:
        raise InvalidHash

    file_size = file_id.file_size
    from_bytes, until_bytes = parse_range_header(range_header, file_size)
    req_length = until_bytes - from_bytes + 1

    chunk_size = 1024 * 1024
    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = (until_bytes % chunk_size) + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)

    # Generate deterministic stream_id based on file identity + user token
    # This ensures:
    # 1. Multiple concurrent HTTP connections from same user = same stream_id
    # 2. Different users watching same file = different stream_id's
    import hashlib
    token = request.path_params.get("token", "unknown")
    stream_id_base = f"{chat_id}:{id}:{token}"
    stream_id = hashlib.md5(stream_id_base.encode()).hexdigest()[:16]
    meta = {
        "request_path": str(request.url.path),
        "client_host": request.client.host if request.client else None,
    }

    # Start usage tracking in background
    asyncio.create_task(track_usage_from_stats(stream_id, request.path_params.get("token"), token_data))

    body_gen = tg_connect.prefetch_stream(
        file_id=file_id,
        client_index=client_idx,
        additional_client_indices=additional_client_indices,
        offset=offset,
        first_part_cut=first_part_cut,
        last_part_cut=last_part_cut,
        part_count=part_count,
        chunk_size=chunk_size,
        prefetch=Telegram.PRE_FETCH,
        stream_id=stream_id,
        meta=meta,
        parallelism=Telegram.PARALLEL,
        request=request,
    )


    file_name = file_id.file_name or f"{secrets.token_hex(2)}.unknown"
    mime_type = file_id.mime_type or mimetypes.guess_type(file_name)[0] or "application/octet-stream"
    if not file_id.file_name and "/" in mime_type:
        file_name = f"{secrets.token_hex(2)}.{mime_type.split('/')[1]}"

    headers = {
        "Content-Type": mime_type,
        "Content-Length": str(req_length),
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Disposition": f'inline; filename="{file_name}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600, immutable",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
        "X-Stream-Id": stream_id,
    }

    if range_header:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"
    
    status_code = 206 if range_header else 200
    if range_header:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"
    
    return StreamingResponse(
        status_code=status_code,
        content=body_gen,
        headers=headers,
        media_type=mime_type,
    )