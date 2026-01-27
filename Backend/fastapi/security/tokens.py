
from fastapi import HTTPException
from Backend import db


import time

TOKEN_CACHE = {}  # token -> (data, timestamp)
CACHE_TTL = 60    # 1 minute

async def verify_token(token: str):
    now = time.time()
    
    # Check cache
    if token in TOKEN_CACHE:
        data, ts = TOKEN_CACHE[token]
        if now - ts < CACHE_TTL:
            token_data = data
            # Use cached data
        else:
            token_data = await db.get_api_token(token)
            if token_data:
                TOKEN_CACHE[token] = (token_data, now)
    else:
        token_data = await db.get_api_token(token)
        if token_data:
            TOKEN_CACHE[token] = (token_data, now)

    if not token_data:
        raise HTTPException(status_code=401, detail="Invalid or expired API token")
        
    # Check Limits
    limits = token_data.get("limits", {})
    usage = token_data.get("usage", {})
    daily_limit = limits.get("daily_limit_gb")
    monthly_limit = limits.get("monthly_limit_gb")
    
    if daily_limit and daily_limit > 0:
         current_daily_gb = usage.get("daily", {}).get("bytes", 0) / (1024**3)
         if current_daily_gb >= daily_limit:
             raise HTTPException(status_code=429, detail="Daily usage limit exceeded")

    if monthly_limit and monthly_limit > 0:
         current_monthly_gb = usage.get("monthly", {}).get("bytes", 0) / (1024**3)
         if current_monthly_gb >= monthly_limit:
             raise HTTPException(status_code=429, detail="Monthly usage limit exceeded")

    return token_data
