from asyncio import sleep
from pyrogram.errors import FloodWait, MessageDeleteForbidden
from Backend.logger import LOGGER
from Backend.pyrofork.bot import Helper, multi_clients
from itertools import cycle
from datetime import datetime

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
            last_error = e
            LOGGER.error(f"Error while deleting message {msg_id} in {chat_id} using Client: {client.name}: {e}")
            continue

    # All retries exhausted - add to junk database
    LOGGER.error(f"Failed to delete message {msg_id} in {chat_id} after {max_retries} attempts.")

    # Add all failed deletions to require_user_delete
    if 'last_error' in locals():
        error_msg = str(last_error)
        try:
            from Backend import db
            await db.dbs["tracking"]["require_user_delete"].insert_one({
                "chat_id": chat_id,
                "msg_id": msg_id,
                "created_at": datetime.utcnow(),
                "reason": f"Message deletion failed: {error_msg[:200]}"  # Limit error message length
            })
            LOGGER.info(f"Added message {msg_id} from {chat_id} to junk database due to deletion failure")
        except Exception as db_error:
            LOGGER.error(f"Failed to add failed deletion to junk database: {db_error}")

async def delete_multiple_messages(messages: list):
    """
    Delete multiple messages in parallel using available bots.
    Implements round-robin distribution with batches of up to 30 messages per bot.
    Args:
        messages: List of tuples (chat_id, msg_id)
    """
    import asyncio
    from collections import defaultdict

    if not messages:
        return

    # Group messages by chat_id first
    grouped_by_chat = defaultdict(list)
    for chat_id, msg_id in messages:
        grouped_by_chat[chat_id].append(msg_id)

    # Flatten all messages to distribute among bots in round-robin fashion
    all_messages = [(chat_id, msg_id) for chat_id, msg_ids in grouped_by_chat.items() for msg_id in msg_ids]

    # Group messages by bot in round-robin fashion
    bot_message_groups = defaultdict(list)

    # Create a cycle of available clients
    clients_cycle = cycle(list(multi_clients.values()) + [Helper] if multi_clients else [Helper])

    for chat_id, msg_id in all_messages:
        client = next(clients_cycle)  # Round-robin assignment
        bot_message_groups[client.name].append((chat_id, msg_id))

    # Process each bot's assigned messages in batches of up to 30
    async def process_bot_messages(bot_name, message_list):
        # Group messages by chat_id within this bot's batch
        bot_grouped = defaultdict(list)
        for chat_id, msg_id in message_list:
            bot_grouped[chat_id].append(msg_id)

        # Process each chat's messages in batches of up to 30
        for chat_id, msg_ids in bot_grouped.items():
            # Split into batches of 30
            batch_size = 30
            for i in range(0, len(msg_ids), batch_size):
                chunk = msg_ids[i:i + batch_size]

                # Find the appropriate client for this batch
                clients_list = list(multi_clients.values()) + [Helper] if multi_clients else [Helper]
                target_client = next((c for c in clients_list if c.name == bot_name), Helper)

                # Attempt deletion with retries
                max_retries = max(len(multi_clients), 3) if multi_clients else 3
                success = False
                failed_bots = []  # Track which bots failed
                last_error = None

                for attempt in range(max_retries):
                    try:
                        await target_client.delete_messages(chat_id=chat_id, message_ids=chunk)
                        success = True
                        LOGGER.info(f"Deleted {len(chunk)} messages in {chat_id} using {target_client.name}")
                        break
                    except FloodWait as e:
                        wait_time = e.value
                        LOGGER.warning(f"FloodWait {wait_time}s in batch delete {chat_id} using {target_client.name}")
                        if attempt >= max_retries - 1:
                            await sleep(wait_time + 1)
                            try:
                                await target_client.delete_messages(chat_id=chat_id, message_ids=chunk)
                                success = True
                            except Exception as final_e:
                                last_error = str(final_e)
                                failed_bots.append(target_client.name)
                        else:
                            await sleep(wait_time)
                    except Exception as e:
                        # Collect failed bot name silently (no per-bot logging)
                        if target_client.name not in failed_bots:
                            failed_bots.append(target_client.name)
                        last_error = str(e)
                        # Try next client on failure
                        target_client = get_next_client()

                if not success:
                    # Single consolidated error log
                    error_short = last_error.split("Pyrogram")[0].strip() if last_error else "Unknown error"
                    LOGGER.error(f"Batch delete failed in {chat_id}: {len(chunk)} msgs, tried bots [{', '.join(failed_bots[:5])}{'...' if len(failed_bots) > 5 else ''}] - {error_short}")
                    
                    # Add all failed messages from this chunk to junk database
                    if last_error:
                        try:
                            from Backend import db
                            error_msg = last_error[:200]  # Limit error message length
                            for msg_id in chunk:
                                try:
                                    await db.dbs["tracking"]["require_user_delete"].insert_one({
                                        "chat_id": chat_id,
                                        "msg_id": msg_id,
                                        "created_at": datetime.utcnow(),
                                        "reason": f"Bulk deletion failed: {error_msg}"
                                    })
                                except Exception as insert_error:
                                    LOGGER.error(f"Failed to insert msg {msg_id} to junk database: {insert_error}")
                            LOGGER.info(f"Added {len(chunk)} failed messages from {chat_id} to junk database")
                        except Exception as db_error:
                            LOGGER.error(f"Failed to add failed batch deletions to junk database: {db_error}")

    # Create tasks for each bot's message group
    tasks = []
    for bot_name, message_list in bot_message_groups.items():
        tasks.append(process_bot_messages(bot_name, message_list))

    if tasks:
        await asyncio.gather(*tasks)


# Note: The process_pending_deletions function is removed as these messages
# can only be deleted by user sessions, not normal bot sessions.
# The require_user_delete collection is meant for user sessions to handle.
