from asyncio import sleep
from pyrogram.errors import FloodWait, MessageDeleteForbidden
from Backend.logger import LOGGER
from Backend.pyrofork.bot import Helper, multi_clients
from itertools import cycle

async def edit_message(chat_id: int, msg_id: int, new_caption: str):
    try:
        await Helper.edit_message_caption(
            chat_id=chat_id,
            message_id=msg_id,
            caption=new_caption
        )
        await sleep(2)
    except FloodWait as e:
        LOGGER.warning(f"FloodWait for {e.value} seconds while editing message {msg_id} in {chat_id}")
        await sleep(e.value)
    except Exception as e:
        LOGGER.error(f"Error while editing message {msg_id} in {chat_id}: {e}")

_client_cycle = None

def get_next_client():
    global _client_cycle
    
    if _client_cycle is None:
        # Create a list of all available clients including Helper
        clients = [Helper]
        if multi_clients:
            clients.extend(multi_clients.values())
        
        # Remove duplicates if any (though multi_clients shouldn't have Helper inside typically)
        unique_clients = list({c.name: c for c in clients}.values())
        _client_cycle = cycle(unique_clients)
    
    try:
        return next(_client_cycle)
    except StopIteration:
        _client_cycle = cycle([Helper])
        return next(_client_cycle)


async def delete_message(chat_id: int, msg_id: int):
    # Determine max retries based on available clients
    # Loop through all clients twice to ensure we give everyone a chance
    client_count = len(multi_clients) + 1 if multi_clients else 1
    max_retries = max(client_count, 3) # At least 3 attempts

    denied_clients = []

    for attempt in range(max_retries):
        client = get_next_client()
        try:
            await client.delete_messages(
                chat_id=chat_id,
                message_ids=msg_id
            )
            # await sleep(2) # Removed forced sleep to speed up bulk operations

            LOGGER.info(f"Deleted message {msg_id} in {chat_id} using Client: {client.name}")
            return # Success

        except FloodWait as e:
            wait_time = e.value
            LOGGER.warning(f"FloodWait {wait_time}s on Client: {client.name} (attempt {attempt + 1}/{max_retries})")

            if attempt >= max_retries - 1:
                 LOGGER.info(f"All bots hit FloodWait. Waiting {wait_time}s before final retry...")
                 await sleep(wait_time + 1)
                 try:
                     await client.delete_messages(chat_id=chat_id, message_ids=msg_id)
                     LOGGER.info(f"Deleted message {msg_id} in {chat_id} after FloodWait using Client: {client.name}")
                     return
                 except Exception as final_e:
                     LOGGER.error(f"Failed to delete {msg_id} after waiting for FloodWait: {final_e}")
                     break

            continue

        except MessageDeleteForbidden:
            denied_clients.append(client.name)
            continue # Try next client

        except Exception as e:
            LOGGER.error(f"Error while deleting message {msg_id} in {chat_id} using Client: {client.name}: {e}")
            continue

    LOGGER.error(f"Failed to delete message {msg_id} in {chat_id} after {max_retries} attempts.")

async def delete_multiple_messages(messages: list):
    """
    Delete multiple messages in parallel using available bots.
    Args:
        messages: List of tuples (chat_id, msg_id)
    """
    import asyncio
    
    if not messages:
        return
        
    # Group by chat_id
    # {chat_id: [msg_id1, msg_id2, ...]}
    grouped = {}
    for chat_id, msg_id in messages:
        if chat_id not in grouped:
            grouped[chat_id] = []
        grouped[chat_id].append(msg_id)
    
    tasks = []
    
    # We will distribute deletion tasks.
    # Since Pyrogram delete_messages accepts a list of IDs for a chat, 
    # we can just use that feature for efficiency if the bot allows it.
    # However, we need to handle permissions/FloodWait.
    # We can reuse 'delete_message' logic but adapted for batch?
    # Or simplified: Pick a client, try to delete ALL messages for that chat.
    
    # Strategy:
    # For each chat, spawn a task that tries to delete the batch of IDs.
    # Use load balancing logic similar to delete_message but for the whole batch.
    
    async def delete_batch_for_chat(c_id, m_ids):
        # Retry logic similar to delete_message
        client_count = len(multi_clients) + 1 if multi_clients else 1
        max_retries = client_count # Iterate through all bots exactly once
        
        # Split into chunks of 100 to be safe (Telegram limit)
        # Actually Pyrogram handles this? Best to be safe.
        chunk_size = 100
        chunks = [m_ids[i:i + chunk_size] for i in range(0, len(m_ids), chunk_size)]
        
        for chunk in chunks:
            success = False
            for attempt in range(max_retries):
                client = get_next_client()
                try:
                    await client.delete_messages(chat_id=c_id, message_ids=chunk)
                    success = True
                    LOGGER.info(f"Deleted {len(chunk)} messages in {c_id} using {client.name}")
                    break
                except FloodWait as e:
                    LOGGER.warning(f"FloodWait {e.value}s in batch delete {c_id} using {client.name}")
                    if attempt >= max_retries - 1:
                         await sleep(e.value + 1)
                         try:
                             await client.delete_messages(chat_id=c_id, message_ids=chunk)
                             success = True
                         except Exception as final_e:
                             LOGGER.error(f"Failed batch delete in {c_id} after FloodWait: {final_e}")
                    continue
                except Exception as e:
                    LOGGER.error(f"Batch delete error in {c_id} using {client.name}: {e}")
                    continue
            
            if not success:
                LOGGER.error(f"Failed to delete batch of {len(chunk)} messages in {c_id}")

    for chat_id, msg_ids in grouped.items():
        tasks.append(delete_batch_for_chat(chat_id, msg_ids))
        
    if tasks:
        await asyncio.gather(*tasks)


# Note: The process_pending_deletions function is removed as these messages
# can only be deleted by user sessions, not normal bot sessions.
# The require_user_delete collection is meant for user sessions to handle.
