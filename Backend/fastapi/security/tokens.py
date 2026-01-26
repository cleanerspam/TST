
from fastapi import HTTPException
from Backend import db


async def verify_token(token: str):
    token_data = await db.get_api_token(token)
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
