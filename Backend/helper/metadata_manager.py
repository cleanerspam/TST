import time
import asyncio
from Backend import db
from Backend.helper.metadata import fetch_tv_metadata, fetch_movie_metadata
from Backend.helper.metadata import fetch_tv_metadata, fetch_movie_metadata
from Backend.helper.metadata import fetch_tv_metadata, fetch_movie_metadata
from Backend.logger import LOGGER
from Backend.pyrofork.bot import StreamBot, multi_clients
from Backend.helper.encrypt import decode_string
import random
from pyrogram.errors import FloodWait
from pyrogram.file_id import FileId

class MetadataManager:
    def __init__(self):
        self.CANCEL_REQUESTED = False
        self.IS_RUNNING = False
        self.progress_callback = None
        self.status_message = "Idle"
        self.last_progress = {"status": "idle", "done": 0, "total": 0, "elapsed": 0}

    async def run_fix_metadata(self, progress_callback=None, concurrency=20, task_type="full"):
        if self.IS_RUNNING:
            return False, "Already running"
        
        self.IS_RUNNING = True
        self.CANCEL_REQUESTED = False
        self.progress_callback = progress_callback
        self.last_progress = {"status": "starting", "done": 0, "total": 0, "elapsed": 0}
        start_time = time.time()
        
        try:
            # -------------------------
            # Gather totals
            # -------------------------
            total_movies = 0
            total_tv = 0
            for i in range(1, db.current_db_index + 1):
                key = f"storage_{i}"
                if key in db.dbs:
                    total_movies += await db.dbs[key]["movie"].count_documents({})
                    total_tv += await db.dbs[key]["tv"].count_documents({})

            TOTAL = total_movies + total_tv
            DONE = 0

            if self.progress_callback:
                await self.progress_callback("initializing", 0, TOTAL, 0)

            # -------------------------
            # Tunables
            # -------------------------
            TASK_BATCH = concurrency * 2
            PROGRESS_INTERVAL = 5.0

            semaphore = asyncio.Semaphore(concurrency)
            meta_cache = {}
            last_progress_edit = start_time

            async def cached_fetch_movie(title, year, default_id, encoded_string=None, quality=None):
                if default_id:
                    k = ("movie", str(default_id))
                else:
                    k = ("movie", f"title::{title or ''}::year::{year or ''}")

                if k in meta_cache:
                    return meta_cache[k]

                async with semaphore:
                    try:
                        meta = await fetch_movie_metadata(title=title, encoded_string=encoded_string, year=year, quality=quality, default_id=default_id)
                    except Exception as e:
                        LOGGER.warning(f"fetch_movie_metadata error for {title} ({default_id}): {e}")
                        meta = None

                meta_cache[k] = meta
                return meta

            async def cached_fetch_tv(title, season, episode, year, default_id, encoded_string=None, quality=None):
                if default_id:
                    k = ("tv", str(default_id), int(season), int(episode))
                else:
                    k = ("tv", f"title::{title or ''}::year::{year or ''}", int(season), int(episode))

                if k in meta_cache:
                    return meta_cache[k]

                async with semaphore:
                    try:
                        meta = await fetch_tv_metadata(title=title, season=season, episode=episode,
                                                    encoded_string=encoded_string, year=year, quality=quality, default_id=default_id)
                    except Exception as e:
                        LOGGER.warning(f"fetch_tv_metadata error for {title} S{season}E{episode} ({default_id}): {e}")
                        meta = None

                meta_cache[k] = meta
                return meta

            def all_fields_present(meta: dict) -> bool:
                if not meta: return False
                if not (meta.get("poster") or meta.get("backdrop")): return False
                has_desc = meta.get("description") or meta.get("genres") or meta.get("cast")
                if not has_desc: return False
                if meta.get("rate") in [0, None]: return False
                if meta.get("runtime") in [0, None]: return False
                return True

            async def fetch_telegram_file_info(encoded_id):
                try:
                    decoded = await decode_string(encoded_id)
                    chat_id = int(f"-100{decoded['chat_id']}")
                    msg_id = int(decoded['msg_id'])
                    
                    # Round Robin / Random selection for load balancing
                    client = StreamBot
                    if multi_clients:
                        client = random.choice(list(multi_clients.values()))
                    
                    try:
                        message = await client.get_messages(chat_id, msg_id)
                    except FloodWait as e:
                        LOGGER.warning(f"FloodWait {e.value}s on {client.name}. Retrying with another bot...")
                        # Try one failover if multi_clients available
                        if multi_clients:
                            client = random.choice(list(multi_clients.values()))
                            message = await client.get_messages(chat_id, msg_id)
                        else:
                            await asyncio.sleep(e.value)
                            message = await client.get_messages(chat_id, msg_id)
                            
                    if not message or message.empty:
                        return None, None
                        
                    file = message.video or message.document
                    if not file:
                        return None, None
                    
                    # Extract dc_id from file_id
                    try:
                        file_id_obj = FileId.decode(file.file_id)
                        dc_id = file_id_obj.dc_id
                    except Exception as e:
                        LOGGER.warning(f"Failed to extract dc_id: {e}")
                        dc_id = None
                        
                    file_type = "video" if message.video else "document"
                    return dc_id, file_type
                except Exception as e:
                    LOGGER.warning(f"Failed to fetch telegram info: {e}")
                    return None, None

            async def _safe_update_movie(collection, movie_doc):
                nonlocal DONE, last_progress_edit
                if self.CANCEL_REQUESTED: return
                
                try:
                    doc_id = movie_doc.get("_id")
                    title = movie_doc.get("title")
                    
                    update_query = {}
                    current = dict(movie_doc)

                    # -----------------
                    # 1. Backfill File Info (DC/Type)
                    # -----------------
                    telegram_files = movie_doc.get("telegram", [])
                    files_updated = False
                    new_telegram = []
                    
                    for f in telegram_files:
                        # Check if missing info
                        # We assume if dc_id is missing, we need to fetch
                        if not f.get("dc_id"):
                            dc, ftype = await fetch_telegram_file_info(f.get("id"))
                            if dc:
                                f["dc_id"] = dc
                                f["file_type"] = ftype
                                files_updated = True
                        new_telegram.append(f)
                        
                    if files_updated:
                        update_query["telegram"] = new_telegram
                        current["telegram"] = new_telegram

                    # -----------------
                    # 2. Metadata Update (only if task_type='full')
                    # -----------------
                    if task_type == "full":
                        imdb_id = movie_doc.get("imdb_id")
                        tmdb_id = movie_doc.get("tmdb_id")
                        year = movie_doc.get("release_year")

                        meta_primary = None
                        meta_secondary = None

                        if imdb_id:
                            meta_primary = await cached_fetch_movie(title, year, imdb_id)
                            fetched_tmdb = meta_primary.get("tmdb_id") if meta_primary else None
                            if (tmdb_id or fetched_tmdb) and (not all_fields_present(meta_primary)):
                                meta_secondary = await cached_fetch_movie(title, year, (tmdb_id or fetched_tmdb))
                        elif tmdb_id:
                            meta_primary = await cached_fetch_movie(title, year, tmdb_id)
                            fetched_imdb = meta_primary.get("imdb_id") if meta_primary else None
                            if fetched_imdb and (not all_fields_present(meta_primary)):
                                meta_secondary = await cached_fetch_movie(title, year, fetched_imdb)
                        else:
                            meta_primary = await cached_fetch_movie(title, year, None)
                            if meta_primary:
                                fetched_imdb = meta_primary.get("imdb_id")
                                fetched_tmdb = meta_primary.get("tmdb_id")
                                if fetched_imdb and (not all_fields_present(meta_primary)):
                                    meta_secondary = await cached_fetch_movie(title, year, fetched_imdb)
                                elif fetched_tmdb and (not all_fields_present(meta_primary)):
                                    meta_secondary = await cached_fetch_movie(title, year, fetched_tmdb)

                        api_map = {
                            "imdb_id": "imdb_id", "tmdb_id": "tmdb_id", "rate": "rating", "cast": "cast",
                            "description": "description", "genres": "genres", "poster": "poster",
                            "backdrop": "backdrop", "runtime": "runtime", "logo": "logo"
                        }

                        for meta in (meta_primary, meta_secondary):
                            if not meta: continue
                            for api_key, db_key in api_map.items():
                                new_val = meta.get(api_key)
                                if new_val is not None:
                                    update_query[db_key] = new_val
                                    current[db_key] = new_val

                    if update_query:
                        filter_q = {"_id": doc_id} 
                        try:
                            await collection.update_one(filter_q, {"$set": update_query})
                        except Exception as e:
                            LOGGER.error(f"DB update failed for movie {title}: {e}")
                            
                    DONE += 1
                    # Progress reporting...
                    now = time.time()
                    if now - last_progress_edit > PROGRESS_INTERVAL:
                        last_progress_edit = now
                        if self.progress_callback:
                            await self.progress_callback("running", DONE, TOTAL, now - start_time)

                except Exception as e:
                    LOGGER.error(f"Error updating movie {movie_doc.get('title')}: {e}")
                    DONE += 1

            async def _safe_update_tv(collection, tv_doc):
                nonlocal DONE, last_progress_edit
                if self.CANCEL_REQUESTED: return

                try:
                    doc_id = tv_doc.get("_id")
                    title = tv_doc.get("title")
                    year = tv_doc.get("release_year")
                    final_imdb = None
                    
                    update_query = {}
                    current = dict(tv_doc)

                    # -----------------
                    # 1. Backfill File Info (DC/Type) for TV
                    # -----------------
                    seasons = tv_doc.get("seasons", [])
                    seasons_updated = False
                    
                    # Iterate and update in place (careful, but valid for dicts in list)
                    for season in seasons:
                        for ep in season.get("episodes", []):
                            telegram_files = ep.get("telegram", [])
                            for f in telegram_files:
                                if not f.get("dc_id"):
                                    dc, ftype = await fetch_telegram_file_info(f.get("id"))
                                    if dc:
                                        f["dc_id"] = dc
                                        f["file_type"] = ftype
                                        seasons_updated = True
                    
                    if seasons_updated:
                        update_query["seasons"] = seasons
                        current["seasons"] = seasons

                    # -----------------
                    # 2. Metadata Update (only if task_type='full')
                    # -----------------
                    if task_type == "full":
                        imdb_id = tv_doc.get("imdb_id")
                        tmdb_id = tv_doc.get("tmdb_id")
                        # year available from top

                        meta_primary = None
                        meta_secondary = None

                        if imdb_id:
                            meta_primary = await cached_fetch_tv(title, 1, 1, year, imdb_id)
                            fetched_tmdb = meta_primary.get("tmdb_id") if meta_primary else None
                            if (tmdb_id or fetched_tmdb) and (not all_fields_present(meta_primary)):
                                meta_secondary = await cached_fetch_tv(title, 1, 1, year, (tmdb_id or fetched_tmdb))
                        elif tmdb_id:
                            meta_primary = await cached_fetch_tv(title, 1, 1, year, tmdb_id)
                            fetched_imdb = meta_primary.get("imdb_id") if meta_primary else None
                            if fetched_imdb and (not all_fields_present(meta_primary)):
                                meta_secondary = await cached_fetch_tv(title, 1, 1, year, fetched_imdb)
                        else:
                            meta_primary = await cached_fetch_tv(title, 1, 1, year, None)
                            if meta_primary:
                                fetched_imdb = meta_primary.get("imdb_id")
                                fetched_tmdb = meta_primary.get("tmdb_id")
                                if fetched_imdb and (not all_fields_present(meta_primary)):
                                    meta_secondary = await cached_fetch_tv(title, 1, 1, year, fetched_imdb)
                                elif fetched_tmdb and (not all_fields_present(meta_primary)):
                                    meta_secondary = await cached_fetch_tv(title, 1, 1, year, fetched_tmdb)


                        api_map = {
                            "imdb_id": "imdb_id", "tmdb_id": "tmdb_id", "rate": "rating", "cast": "cast",
                            "description": "description", "genres": "genres", "poster": "poster",
                            "backdrop": "backdrop", "runtime": "runtime", "logo": "logo"
                        }

                        for meta in (meta_primary, meta_secondary):
                            if not meta: continue
                            for api_key, db_key in api_map.items():
                                new_val = meta.get(api_key)
                                if new_val is not None:
                                    update_query[db_key] = new_val
                                    current[db_key] = new_val

                    # Apply Main Update
                    if update_query:
                        filter_q = {"_id": doc_id} 
                        try:
                            await collection.update_one(filter_q, {"$set": update_query})
                        except Exception as e:
                            LOGGER.error(f"DB update failed for TV {title}: {e}")

                    # -----------------
                    # 3. Episode Updates (only if task_type='full')
                    # -----------------
                    if task_type == "full":
                        final_imdb = current.get("imdb_id")
                        if not final_imdb:
                            DONE += 1
                            return

                        ep_tasks = []
                        # Use updated seasons if we have them, else from doc
                        target_seasons = current.get("seasons", [])
                        
                        for season in target_seasons:
                            s_num = season.get("season_number")
                            for ep in season.get("episodes", []):
                                e_num = ep.get("episode_number")

                                # skip if episode appears complete
                                if ep.get("overview") and ep.get("released") and ep.get("episode_backdrop"):
                                    continue

                                async def ep_task(sn=s_num, en=e_num):
                                    try:
                                        meta = await cached_fetch_tv(title, sn, en, year, final_imdb)
                                        if not meta: return

                                        ep_update = {}
                                        if meta.get("episode_overview"):
                                            ep_update["seasons.$[s].episodes.$[e].overview"] = meta["episode_overview"]
                                        if meta.get("episode_released"):
                                            ep_update["seasons.$[s].episodes.$[e].released"] = meta["episode_released"]
                                        if meta.get("episode_backdrop"):
                                            ep_update["seasons.$[s].episodes.$[e].episode_backdrop"] = meta["episode_backdrop"]

                                        if ep_update:
                                            filt = {"_id": doc_id} 
                                            await collection.update_one(
                                                filt,
                                                {"$set": ep_update},
                                                array_filters=[
                                                    {"s.season_number": sn},
                                                    {"e.episode_number": en}
                                                ]
                                            )
                                    except Exception as e:
                                        # LOGGER.error(f"Error updating episode {title} S{sn}E{en}: {e}")
                                        pass

                                ep_tasks.append(ep_task())

                        # Execute Ep Tasks
                        for i in range(0, len(ep_tasks), TASK_BATCH):
                            if self.CANCEL_REQUESTED: break
                            batch = ep_tasks[i:i+TASK_BATCH]
                            running = [asyncio.create_task(t) for t in batch]
                            await asyncio.gather(*running, return_exceptions=True)



                    DONE += 1
                    now = time.time()
                    if now - last_progress_edit > PROGRESS_INTERVAL:
                        last_progress_edit = now
                        if self.progress_callback:
                            await self.progress_callback("running", DONE, TOTAL, now - start_time)

                except Exception as e:
                    LOGGER.error(f"Error updating TV show {tv_doc.get('title')}: {e}")
                    DONE += 1

            # -------------------------
            # ORCHESTRATION
            # -------------------------
            async def update_movies():
                for i in range(1, db.current_db_index + 1):
                    if self.CANCEL_REQUESTED: break
                    key = f"storage_{i}"
                    if key not in db.dbs: continue
                    collection = db.dbs[key]["movie"]
                    
                    last_id = None
                    while True:
                        if self.CANCEL_REQUESTED: break
                        
                        # Keyset Pagination: Fetch batch of 20
                        query = {"_id": {"$gt": last_id}} if last_id else {}
                        # Sort by _id to ensure stable ordering
                        cursor = collection.find(query).sort("_id", 1).limit(TASK_BATCH)
                        
                        batch = await cursor.to_list(length=TASK_BATCH)
                        if not batch:
                            break
                            
                        tasks = []
                        for movie in batch:
                            last_id = movie["_id"]
                            if self.CANCEL_REQUESTED: break
                            tasks.append(_safe_update_movie(collection, movie))
                        
                        # Process batch concurrently
                        if tasks:
                            await asyncio.gather(*tasks, return_exceptions=True)

            async def update_tv_shows():
                for i in range(1, db.current_db_index + 1):
                    if self.CANCEL_REQUESTED: break
                    key = f"storage_{i}"
                    if key not in db.dbs: continue
                    collection = db.dbs[key]["tv"]
                    
                    last_id = None
                    while True:
                        if self.CANCEL_REQUESTED: break
                        
                        # Keyset Pagination
                        query = {"_id": {"$gt": last_id}} if last_id else {}
                        cursor = collection.find(query).sort("_id", 1).limit(TASK_BATCH)
                        
                        batch = await cursor.to_list(length=TASK_BATCH)
                        if not batch:
                            break
                            
                        tasks = []
                        for tv in batch:
                            last_id = tv["_id"]
                            if self.CANCEL_REQUESTED: break
                            tasks.append(_safe_update_tv(collection, tv))
                            
                        # Process batch concurrently
                        if tasks:
                            await asyncio.gather(*tasks, return_exceptions=True)

            await asyncio.gather(update_movies(), update_tv_shows())

            if self.CANCEL_REQUESTED:
                if self.progress_callback:
                    await self.progress_callback("cancelled", DONE, TOTAL, time.time() - start_time)
                return False, "Cancelled by user"

            if self.progress_callback:
                await self.progress_callback("completed", DONE, TOTAL, time.time() - start_time)
            
            return True, "Completed successfully"

        except Exception as e:
            LOGGER.exception(f"Error in metadata run: {e}")
            if self.progress_callback:
                await self.progress_callback("error", DONE, TOTAL, time.time() - start_time)
            return False, str(e)
        finally:
            self.IS_RUNNING = False

    def cancel(self):
        self.CANCEL_REQUESTED = True

# Global instance
metadata_manager = MetadataManager()
