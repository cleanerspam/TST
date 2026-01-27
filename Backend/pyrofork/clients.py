from asyncio import sleep, create_task
from pyrogram import Client
from pyrogram.errors import FloodWait
from Backend.logger import LOGGER
from Backend.config import Telegram
from Backend.pyrofork.bot import multi_clients, work_loads, StreamBot, client_dc_map
from os import environ

class TokenParser:
    @staticmethod
    def parse_from_env():
        tokens = {
            c + 1: t
            for c, (_, t) in enumerate(
                filter(
                    lambda n: n[0].startswith("MULTI_TOKEN"), 
                    sorted(environ.items())
                )
            )
        }
        return tokens

async def wait_and_start(client_id, token, wait_time):
    LOGGER.warning(f"Client {client_id} hit FloodWait. Retrying in {wait_time}s...")
    await sleep(wait_time + 5) # Add 5s buffer
    cid, client = await start_client(client_id, token)
    if client:
        multi_clients[cid] = client
        LOGGER.info(f"Client {cid} initialized late after FloodWait")

async def start_client(client_id, token):
    try:
        LOGGER.info(f"Starting - Bot Client {client_id}")
        client = await Client(
            name=str(client_id),
            api_id=Telegram.API_ID,
            api_hash=Telegram.API_HASH,
            bot_token=token,
            sleep_threshold=100,
            no_updates=True,
            in_memory=True,
            max_concurrent_transmissions=100
        ).start()
        
        try:
            client_dc = await client.storage.dc_id()
            client_dc_map[client_id] = client_dc
            LOGGER.info(f"Client {client_id} connected to DC {client_dc}")
        except Exception as e:
            LOGGER.warning(f"Could not get DC for Client {client_id}: {e}")
            client_dc_map[client_id] = None

        work_loads[client_id] = 0
        return client_id, client
    except FloodWait as e:
        create_task(wait_and_start(client_id, token, e.value))
        return client_id, None
    except Exception as e:
        LOGGER.error(f"Failed to start Client - {client_id} Error: {e}")
        return client_id, None

async def initialize_clients():
    multi_clients[0], work_loads[0] = StreamBot, 0

    try:
        main_dc = await StreamBot.storage.dc_id()
        client_dc_map[0] = main_dc
        LOGGER.info(f"Main StreamBot connected to DC {main_dc}")
    except Exception as e:
        LOGGER.warning(f"Could not get DC for StreamBot: {e}")
        client_dc_map[0] = None

    all_tokens = TokenParser.parse_from_env()
    if not all_tokens:
        LOGGER.info("No additional Bot Clients found, Using default client")
        return

    LOGGER.info(f"Found {len(all_tokens)} additional clients. Initializing safely...")
    
    for i, token in all_tokens.items():
        try:
            cid, client = await start_client(i, token)
            if client:
                multi_clients[cid] = client
            
            # Delay to prevent FloodWait from Telegram
            await sleep(2)
        except Exception as e:
            LOGGER.error(f"Critical error initializing client {i}: {e}")

    if len(multi_clients) != 1:
        LOGGER.info(f"Multi-Client Mode Enabled with {len(multi_clients)} clients. DC Map: {client_dc_map}")
    else:
        LOGGER.info("No additional clients were initialized, using default client")

