from pyrogram import Client
from Backend.config import Telegram


StreamBot = Client(
    name='bot',
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    bot_token=Telegram.BOT_TOKEN,
    plugins={"root": "Backend/pyrofork/plugins"},
    sleep_threshold=20,
    workers=Telegram.WORKERS,
    max_concurrent_transmissions=100
)


Helper = Client(
    "helper",
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    bot_token=Telegram.HELPER_BOT_TOKEN,
    sleep_threshold=20,
    workers=Telegram.WORKERS,
    max_concurrent_transmissions=100
)


multi_clients = {}
work_loads = {}
client_dc_map = {}