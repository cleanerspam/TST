
from asyncio import create_task
from bson import ObjectId
import motor.motor_asyncio
from datetime import datetime, timedelta, timezone
from pydantic import ValidationError
from pymongo import ASCENDING, DESCENDING
from typing import Dict, List, Optional, Tuple, Any

from Backend.logger import LOGGER
from Backend.config import Telegram
import re
from Backend.helper.encrypt import decode_string, encode_string
from Backend.helper.modal import Episode, MovieSchema, QualityDetail, Season, TVShowSchema, PendingUpdateSchema
from Backend.helper.task_manager import delete_message
import secrets
import string
from pyrogram.errors import FloodWait
from pyrogram.file_id import FileId


def convert_objectid_to_str(document: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in document.items():
        if isinstance(value, ObjectId):
            document[key] = str(value)
        elif isinstance(value, list):
            document[key] = [convert_objectid_to_str(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict):
            document[key] = convert_objectid_to_str(value)
    return document


def _normalize_filename(filename: str) -> str:
    """
    Normalize filename for fuzzy matching to handle naming variations.
    
    Handles:
    - Lowercase conversion
    - Underscore to space conversion
    - Multiple/extra whitespace removal
    - Double extensions (e.g., .mkv.mkv -> .mkv)
    """
    if not filename:
        return ""
    
    # Convert to lowercase for case-insensitive comparison
    normalized = filename.lower().strip()
    
    # Replace underscores with spaces
    normalized = normalized.replace('_', ' ')
    
    # Remove multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Handle double extensions (e.g., .mkv.mkv -> .mkv)
    # Match pattern: .ext.ext at the end
    normalized = re.sub(r'\.(\w+)\.(\1)$', r'.\1', normalized)
    
    return normalized


def _is_fuzzy_duplicate(existing_file: dict, new_file: dict) -> bool:
    """
    Check if two files are fuzzy duplicates (same normalized name and size).
    Returns True if files match after normalization.
    """
    existing_name_normalized = _normalize_filename(existing_file.get("name", ""))
    new_name_normalized = _normalize_filename(new_file.get("name", ""))
    
    return (existing_name_normalized == new_name_normalized and 
            existing_file.get("size") == new_file.get("size"))


def _should_delete_existing(existing_file: dict, new_file: dict) -> bool:
    """
    Determine if existing file should be deleted instead of new file.
    Returns True if we should delete the EXISTING file (keep new one).
    
    Logic: Prefer video files over document files for streaming.
    """
    existing_type = existing_file.get("file_type", "video")
    new_type = new_file.get("file_type", "video")
    
    # If older is document and newer is video, delete the older document
    if existing_type == "document" and new_type == "video":
        return True
    
    # Otherwise keep existing, delete new
    return False



class Database:
    def __init__(self, db_name: str = "dbFyvio"):
        self.db_uris = Telegram.DATABASE
        self.db_name = db_name

        if len(self.db_uris) < 2:
            raise ValueError("At least 2 database URIs are required (1 for tracking + 1 for storage).")

        self.clients: Dict[str, motor.motor_asyncio.AsyncIOMotorClient] = {}
        self.dbs: Dict[str, motor.motor_asyncio.AsyncIOMotorDatabase] = {}

        self.current_db_index = 1
        
        # In-memory task tracking
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        self.bot_client = None

    def set_bot_client(self, client):
        self.bot_client = client

    async def connect(self):
        try:
            for index, uri in enumerate(self.db_uris):
                client = motor.motor_asyncio.AsyncIOMotorClient(uri)
                db_key = "tracking" if index == 0 else f"storage_{index}"
                self.clients[db_key] = client
                self.dbs[db_key] = client[self.db_name]
                db_type = "Tracking" if index == 0 else f"Storage {index}"

                masked_uri = re.sub(r"://(.*?):.*?@", r"://\1:*****@", uri)
                masked_uri = masked_uri.split('?')[0]
                
                LOGGER.info(f"{db_type} Database connected successfully: {masked_uri}")

            state = await self.dbs["tracking"]["state"].find_one({"_id": "db_index"})
            if not state:
                await self.dbs["tracking"]["state"].insert_one({"_id": "db_index", "current_index": 1})
                self.current_db_index = 1
            else:
                self.current_db_index = state["current_index"]

            LOGGER.info(f"Active storage DB: storage_{self.current_db_index}")

        except Exception as e:
            LOGGER.error(f"Database connection error: {e}")

    async def disconnect(self):
        for client in self.clients.values():
            client.close()
        LOGGER.info("All database connections closed.")

    async def update_current_db_index(self):
        await self.dbs["tracking"]["state"].update_one(
            {"_id": "db_index"},
            {"$set": {"current_index": self.current_db_index}},
            upsert=True
        )

    async def _fetch_telegram_file_info(self, encoded_id):
        """
        Fetch file info from Telegram if missing in DB.
        Uses self.bot_client injected at startup.
        """
        if not self.bot_client:
            LOGGER.warning("Bot client not set in Database - skipping file info fetch")
            return None, None

        try:
            decoded = await decode_string(encoded_id)
            chat_id = int(f"-100{decoded['chat_id']}")
            msg_id = int(decoded['msg_id'])
            
            # Use the injected client (load balancing handled by client pool if implemented there, 
            # but here we just use the main bot instance passed in)
            client = self.bot_client
            
            try:
                message = await client.get_messages(chat_id, msg_id)
            except FloodWait as e:
                LOGGER.warning(f"FloodWait {e.value}s on fetch_info. Sleeping...")
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


    # -------------------------------
    # Helper Methods for Repeated Logic
    # -------------------------------
    def _get_sort_dict(self, sort_params: List[Tuple[str, str]]) -> Dict[str, int]:
        if sort_params:
            sort_field, sort_direction = sort_params[0]
            return {sort_field: DESCENDING if sort_direction.lower() == "desc" else ASCENDING}
        return {"updated_on": DESCENDING}

    async def _paginate_collection(
        self,
        collection_name: str,
        sort_dict: Dict[str, int],
        page: int,
        page_size: int,
        filter_dict: Optional[dict] = None
    ):
        filter_dict = filter_dict or {}
        skip = (page - 1) * page_size
        results = []
        dbs_checked = []
        total_count = 0

        db_counts = []
        for i in range(1, self.current_db_index + 1):
            db_key = f"storage_{i}"
            db = self.dbs[db_key]
            count = await db[collection_name].count_documents(filter_dict)
            db_counts.append((i, count))
            total_count += count

        start_db_index = None
        for db_index, count in reversed(db_counts):
            if skip < count:
                start_db_index = db_index
                break
            skip -= count

        if not start_db_index:
            return [], [], total_count

        for db_index, count in reversed(db_counts):
            if db_index < start_db_index:
                continue

            db_key = f"storage_{db_index}"
            db = self.dbs[db_key]
            dbs_checked.append(db_index)

            cursor = (
                db[collection_name]
                .find(filter_dict)
                .sort(sort_dict)
                .skip(skip if db_index == start_db_index else 0)
                .limit(page_size - len(results))
            )

            docs = await cursor.to_list(None)
            results.extend(docs)

            if len(results) >= page_size:
                break

        return results, dbs_checked, total_count



    async def _move_document(
        self, collection_name: str, document: dict, old_db_index: int
    ) -> bool:
        current_db_key = f"storage_{self.current_db_index}"
        old_db_key = f"storage_{old_db_index}"
        document["db_index"] = self.current_db_index
        try:
            await self.dbs[current_db_key][collection_name].insert_one(document)
            await self.dbs[old_db_key][collection_name].delete_one({"_id": document["_id"]})
            LOGGER.info(f"✅ Moved document {document.get('tmdb_id')} from {old_db_key} to {current_db_key}")
            return True
        except Exception as e:
            LOGGER.error(f"Error moving document to {current_db_key}: {e}")
            return False

    async def _handle_storage_error(self, func, *args, total_storage_dbs: int) -> Optional[Any]:
        next_db_index = (self.current_db_index % total_storage_dbs) + 1
        if next_db_index == 1:
            LOGGER.warning("⚠️ All storage databases are full! Add more.")
            return None
        self.current_db_index = next_db_index
        await self.update_current_db_index()
        LOGGER.info(f"Switched to storage_{self.current_db_index}")
        return await func(*args)

    async def _find_existing_media(self, collection_name: str, imdb_id: str = None, tmdb_id: int = None, title: str = None, release_year: int = None):
        """
        Helper to find existing media across all storage shards.
        Returns: (document, db_key, db_index) or (None, None, None)
        """
        total_storage_dbs = len(self.dbs) - 1
        for db_index in range(1, total_storage_dbs + 1):
            db_key = f"storage_{db_index}"
            media = None
            if imdb_id:
                media = await self.dbs[db_key][collection_name].find_one({"imdb_id": imdb_id})
            if not media and tmdb_id:
                media = await self.dbs[db_key][collection_name].find_one({"tmdb_id": tmdb_id})
            if not media and title and release_year:
                media = await self.dbs[db_key][collection_name].find_one({
                    "title": title, 
                    "release_year": release_year
                })
            
            if media:
                return media, db_key, db_index
        
        return None, None, None

    async def _delete_telegram_file_async(self, file_id_str: str, log_name: str = "file"):
        """Safely delete a file from Telegram channel."""
        if not file_id_str:
            return
        try:
            decoded_data = await decode_string(file_id_str)
            chat_id = int(f"-100{decoded_data['chat_id']}")
            msg_id = int(decoded_data['msg_id'])
            create_task(delete_message(chat_id, msg_id))
            if log_name:
                LOGGER.info(f"Queued deletion of {log_name}")
        except Exception as e:
            LOGGER.error(f"Failed to queue deletion for {log_name}: {e}")



    # -------------------------------
    # Multi Database Method for insert/update/delete/list
    # -------------------------------

    async def insert_media(
        self, metadata_info: dict,
        channel: int, msg_id: int, size: str, name: str,
        dc_id: int = None, file_type: str = "video"
    ) -> Optional[ObjectId]:
        
        if metadata_info['media_type'] == "movie":
            media = MovieSchema(
                tmdb_id=metadata_info['tmdb_id'],
                imdb_id=metadata_info['imdb_id'],
                db_index=self.current_db_index,
                title=metadata_info['title'],
                genres=metadata_info['genres'],
                description=metadata_info['description'],
                rating=metadata_info['rate'],
                release_year=metadata_info['year'],
                poster=metadata_info['poster'],
                backdrop=metadata_info['backdrop'],
                logo=metadata_info['logo'],
                cast=metadata_info['cast'],
                runtime=metadata_info['runtime'],
                media_type=metadata_info['media_type'],
                telegram=[QualityDetail(
                    quality=metadata_info['quality'],
                    id=metadata_info['encoded_string'],
                    name=name,
                    size=size,
                    dc_id=dc_id,
                    file_type=file_type
                )]
            )
            return await self.update_movie(media)
        else:
            tv_show = TVShowSchema(
                tmdb_id=metadata_info['tmdb_id'],
                imdb_id=metadata_info['imdb_id'],
                db_index=self.current_db_index,
                title=metadata_info['title'],
                genres=metadata_info['genres'],
                description=metadata_info['description'],
                rating=metadata_info['rate'],
                release_year=metadata_info['year'],
                poster=metadata_info['poster'],
                backdrop=metadata_info['backdrop'],
                logo=metadata_info['logo'],
                cast=metadata_info['cast'],
                runtime=metadata_info['runtime'],
                media_type=metadata_info['media_type'],
                seasons=[Season(
                    season_number=metadata_info['season_number'],
                    episodes=[Episode(
                        episode_number=metadata_info['episode_number'],
                        title=metadata_info['episode_title'],
                        episode_backdrop=metadata_info['episode_backdrop'],
                        overview=metadata_info['episode_overview'],
                        released=metadata_info['episode_released'],
                        telegram=[QualityDetail(
                            quality=metadata_info['quality'],
                            id=metadata_info['encoded_string'],
                            name=name,
                            size=size,
                            dc_id=dc_id,
                            file_type=file_type
                        )]
                    )]
                )]
            )
            return await self.update_tv_show(tv_show)

    async def insert_pending_update(self, pending_data: PendingUpdateSchema):
        try:
            pending_dict = pending_data.dict()
            await self.dbs["tracking"]["pending_updates"].insert_one(pending_dict)
            LOGGER.info(f"Update pushed to pending list: {pending_dict['metadata']['title']}")
            return "PENDING"
        except Exception as e:
            LOGGER.error(f"Failed to insert pending update: {e}")
            return None

    async def update_movie(self, movie_data: MovieSchema, force_update: bool = False) -> Optional[ObjectId]:
        try:
            movie_dict = movie_data.dict()
        except ValidationError as e:
            LOGGER.error(f"Validation error: {e}")
            return None

        imdb_id = movie_dict.get("imdb_id")
        tmdb_id = movie_dict.get("tmdb_id")
        title = movie_dict.get("title")
        release_year = movie_dict.get("release_year")
        
        quality_to_update = movie_dict["telegram"][0]
        target_quality = quality_to_update["quality"]
        
        total_storage_dbs = len(self.dbs) - 1
        current_db_key = f"storage_{self.current_db_index}"
        
        # 1. Find Existing Movie
        existing_movie, existing_db_key, existing_db_index = await self._find_existing_media(
            "movie", imdb_id, tmdb_id, title, release_year
        )

        # 2. Insert New if Not Found
        if not existing_movie:
            try:
                movie_dict["db_index"] = self.current_db_index
                result = await self.dbs[current_db_key]["movie"].insert_one(movie_dict)
                return result.inserted_id
            except Exception as e:
                LOGGER.error(f"Insertion failed in {current_db_key}: {e}")
                if "storage" in str(e).lower() or "quota" in str(e).lower():
                    return await self._handle_storage_error(self.update_movie, movie_data, total_storage_dbs=total_storage_dbs)
                return None

        # 3. Handle Existing Movie
        movie_id = existing_movie["_id"]
        existing_qualities = existing_movie.get("telegram", [])
        matching_quality = next((q for q in existing_qualities if q["quality"] == target_quality), None)

        if matching_quality and not force_update:
            # Check Fuzzy Duplicate
            if _is_fuzzy_duplicate(matching_quality, quality_to_update):
                delete_existing_file = _should_delete_existing(matching_quality, quality_to_update)
                
                if delete_existing_file:
                    # Delete OLDER document, keep NEWER video
                    await self._delete_telegram_file_async(matching_quality.get("id"), "old document file")
                    
                    # Update DB: Remove old, add new
                    existing_qualities = [q for q in existing_qualities if q["quality"] != target_quality]
                    existing_qualities.append(quality_to_update)
                    
                    existing_movie["telegram"] = existing_qualities
                    existing_movie["updated_on"] = datetime.utcnow()
                    
                    try:
                        await self.dbs[existing_db_key]["movie"].replace_one({"_id": movie_id}, existing_movie)
                        LOGGER.info(f"Replaced document file with video file for {title}")
                        return movie_id
                    except Exception as e:
                        LOGGER.error(f"Failed to update movie db: {e}")
                        return None
                else:
                    # Delete NEWER file (keep existing) -- Auto-delete incoming
                    ex_type = matching_quality.get("file_type", "video")
                    new_type = quality_to_update.get("file_type", "video")
                    log_msg = f"newer file ({new_type})" if ex_type != new_type else "newer file (fuzzy duplicate)"
                    LOGGER.info(f"Fuzzy duplicate: {log_msg}. Deleting {quality_to_update.get('name')}")
                    
                    await self._delete_telegram_file_async(quality_to_update.get("id"), "new duplicate file")
                    return movie_id
            else:
                 # Not a fuzzy duplicate - Push to Pending
                try:
                    LOGGER.info("Duplicate quality found (different characteristics). Pushing to pending...")
                    pending_entry = PendingUpdateSchema(
                        tmdb_id=tmdb_id, media_type="movie", quality=target_quality,
                        new_file=quality_to_update,
                        metadata={
                            "title": title, "year": release_year,
                            "poster": movie_dict.get("poster"), "backdrop": movie_dict.get("backdrop")
                        }
                    )
                    return await self.insert_pending_update(pending_entry)
                except Exception as e:
                    LOGGER.error(f"Failed to queue pending update: {e}")
                    return None
        
        elif matching_quality and force_update:
            LOGGER.info(f"Force update: Replacing {target_quality}")
            await self._delete_telegram_file_async(matching_quality.get("id"), "old file (force update)")
            
            existing_qualities = [q for q in existing_qualities if q["quality"] != target_quality]
            existing_qualities.append(quality_to_update)
        else:
             # Just append new quality
            existing_qualities.append(quality_to_update)

        existing_movie["telegram"] = existing_qualities
        existing_movie["updated_on"] = datetime.utcnow()

        if existing_db_index != self.current_db_index:
            try:
                if await self._move_document("movie", existing_movie, existing_db_index):
                    return movie_id
            except Exception as e:
                LOGGER.error(f"Error moving movie to {current_db_key}: {e}")
                if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                    return await self._handle_storage_error(self.update_movie, movie_data, total_storage_dbs=total_storage_dbs)

        try:
            await self.dbs[existing_db_key]["movie"].replace_one({"_id": movie_id}, existing_movie)
            return movie_id
        except Exception as e:
            LOGGER.error(f"Failed to update movie {tmdb_id} in {existing_db_key}: {e}")
            if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                return await self._handle_storage_error(self.update_movie, movie_data, total_storage_dbs=total_storage_dbs)

    async def update_tv_show(self, tv_show_data: TVShowSchema, force_update: bool = False) -> Optional[ObjectId]:
        try:
            tv_show_dict = tv_show_data.dict()
        except ValidationError as e:
            LOGGER.error(f"Validation error: {e}")
            return None
        
        imdb_id = tv_show_dict.get("imdb_id")
        tmdb_id = tv_show_dict.get("tmdb_id")
        title = tv_show_dict["title"]
        release_year = tv_show_dict["release_year"]
        current_db_key = f"storage_{self.current_db_index}"
        total_storage_dbs = len(self.dbs) - 1

        existing_db_key = None
        existing_db_index = None
        existing_tv = None

        for db_index in range(1, total_storage_dbs + 1):
            db_key = f"storage_{db_index}"
            tv = None
            if imdb_id:
                tv = await self.dbs[db_key]["tv"].find_one({"imdb_id": imdb_id})
            if not tv and tmdb_id:
                tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
            if not tv and title and release_year:
                tv = await self.dbs[db_key]["tv"].find_one({
                    "title": title,
                    "release_year": release_year
                })
            if tv:
                existing_db_key = db_key
                existing_db_index = db_index
                existing_tv = tv
                break

        if not existing_tv:
            try:
                tv_show_dict["db_index"] = self.current_db_index
                result = await self.dbs[current_db_key]["tv"].insert_one(tv_show_dict)
                return result.inserted_id
            except Exception as e:
                LOGGER.error(f"Insertion failed in {current_db_key}: {e}")
                if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                    return await self._handle_storage_error(self.update_tv_show, tv_show_data, total_storage_dbs=total_storage_dbs)
                return None

        tv_id = existing_tv["_id"]
        for season in tv_show_dict["seasons"]:
            existing_season = next(
                (s for s in existing_tv["seasons"] if s["season_number"] == season["season_number"]), None
            )
            if existing_season:
                for episode in season["episodes"]:
                    existing_episode = next(
                        (e for e in existing_season["episodes"] if e["episode_number"] == episode["episode_number"]), None
                    )
                    if existing_episode:
                        existing_episode.setdefault("telegram", [])
                        for quality in episode["telegram"]:
                            existing_quality = next(
                                (q for q in existing_episode["telegram"]
                                if q.get("quality") == quality.get("quality")),
                                None
                            )
                            if existing_quality and not force_update:
                                # Check if this is a fuzzy duplicate (same normalized name + size)
                                if _is_fuzzy_duplicate(existing_quality, quality):
                                    # Fuzzy duplicate detected - decide which file to delete
                                    delete_existing_file = _should_delete_existing(existing_quality, quality)
                                    
                                    if delete_existing_file:
                                        # Delete OLDER document file, keep NEWER video file
                                        try:
                                            old_id = existing_quality.get("id")
                                            if old_id:
                                                decoded_data = await decode_string(old_id)
                                                chat_id = int(f"-100{decoded_data['chat_id']}")
                                                msg_id = int(decoded_data['msg_id'])
                                                create_task(delete_message(chat_id, msg_id))
                                                LOGGER.info(f"Fuzzy duplicate detected for episode (normalized name + size match). Older is document, newer is video. Deleting older document: {existing_quality.get('name')}")
                                        except Exception as e:
                                            LOGGER.error(f"Failed to delete old episode document file: {e}")
                                        
                                        # Replace old document with new video in episode
                                        existing_episode["telegram"] = [q for q in existing_episode["telegram"] if q.get("quality") != quality.get("quality")]
                                        existing_episode["telegram"].append(quality)
                                    else:
                                        # Delete NEWER file (keep existing)
                                        try:
                                            new_id = quality.get("id")
                                            if new_id:
                                                decoded_data = await decode_string(new_id)
                                                chat_id = int(f"-100{decoded_data['chat_id']}")
                                                msg_id = int(decoded_data['msg_id'])
                                                create_task(delete_message(chat_id, msg_id))
                                                
                                                existing_type = existing_quality.get("file_type", "video")
                                                new_type = quality.get("file_type", "video")
                                                if existing_type == new_type:
                                                    LOGGER.info(f"Fuzzy duplicate detected for episode (normalized name + size match, same file type). Auto-deleting newer file: {quality.get('name')}")
                                                else:
                                                    LOGGER.info(f"Fuzzy duplicate detected for episode (normalized name + size match). Older is video, newer is document. Deleting newer document: {quality.get('name')}")
                                        except Exception as e:
                                            LOGGER.error(f"Failed to delete new duplicate episode file: {e}")
                                        
                                        # Don't add to episode, file already deleted
                                        pass
                                else:
                                    # Not a fuzzy duplicate - different file characteristics
                                    # Send to pending updates for manual review
                                    try:
                                        LOGGER.info("Duplicate episode quality found with different file characteristics. Pushing to pending updates...")
                                        
                                        pending_entry = PendingUpdateSchema(
                                            tmdb_id=tmdb_id,
                                            media_type="tv",
                                            quality=quality.get("quality"),
                                            season=season.get("season_number"),
                                            episode=episode.get("episode_number"),
                                            new_file=quality,
                                            metadata={
                                                "title": title,
                                                "year": release_year,
                                                "poster": tv_show_dict.get("poster"),
                                                "backdrop": tv_show_dict.get("backdrop"),
                                                "episode_title": episode.get("title")
                                            }
                                        )
                                        return await self.insert_pending_update(pending_entry)

                                    except Exception as e:
                                        LOGGER.error(f"Failed to queue pending update: {e}")
                                        return None
                            elif existing_quality and force_update:
                                # FORCE UPDATE: Replace the existing quality
                                LOGGER.info(f"Force update: Replacing existing {quality.get('quality')} quality for episode")
                                
                                # Delete old file from Telegram
                                try:
                                    old_id = existing_quality.get("id")
                                    if old_id:
                                        decoded_data = await decode_string(old_id)
                                        chat_id = int(f"-100{decoded_data['chat_id']}")
                                        msg_id = int(decoded_data['msg_id'])
                                        create_task(delete_message(chat_id, msg_id))
                                        LOGGER.info(f"Queued deletion of old episode file: {existing_quality.get('name')}")
                                except Exception as e:
                                    LOGGER.error(f"Failed to queue old episode file for deletion: {e}")
                                
                                # Replace the quality entry
                                existing_episode["telegram"] = [q for q in existing_episode["telegram"] if q.get("quality") != quality.get("quality")]
                                existing_episode["telegram"].append(quality)
                            else:
                                # No matching quality, just append
                                existing_episode["telegram"].append(quality)
                    else:
                        existing_season["episodes"].append(episode)
            else:
                existing_tv["seasons"].append(season)
        existing_tv["updated_on"] = datetime.utcnow()

        if existing_db_index != self.current_db_index:
            try:
                if await self._move_document("tv", existing_tv, existing_db_index):
                    return tv_id
            except Exception as e:
                LOGGER.error(f"Error moving TV show to {current_db_key}: {e}")
                if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                    return await self._handle_storage_error(self.update_tv_show, tv_show_data, total_storage_dbs=total_storage_dbs)
            return tv_id

        try:
            await self.dbs[existing_db_key]["tv"].replace_one({"_id": tv_id}, existing_tv)
            return tv_id
        except Exception as e:
            LOGGER.error(f"Failed to update TV show {tmdb_id} in {existing_db_key}: {e}")
            if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                return await self._handle_storage_error(self.update_tv_show, tv_show_data, total_storage_dbs=total_storage_dbs)
    
    async def sort_movies(self, sort_params, page, page_size, genre_filter=None):
        sort_dict = self._get_sort_dict(sort_params)
        filter_dict = {"genres": {"$in": [genre_filter]}} if genre_filter else {}
        results, dbs_checked, total_count = await self._paginate_collection(
            "movie", sort_dict, page, page_size, filter_dict=filter_dict
        )
        total_pages = (total_count + page_size - 1) // page_size
        return {
            "total_count": total_count,
            "total_pages": total_pages,
            "databases_checked": dbs_checked,
            "current_page": page,
            "movies": [convert_objectid_to_str(result) for result in results],
        }

    async def sort_tv_shows(self, sort_params, page, page_size, genre_filter=None):
        sort_dict = self._get_sort_dict(sort_params)
        filter_dict = {"genres": {"$in": [genre_filter]}} if genre_filter else {}
        results, dbs_checked, total_count = await self._paginate_collection(
            "tv", sort_dict, page, page_size, filter_dict=filter_dict
        )
        total_pages = (total_count + page_size - 1) // page_size
        return {
            "total_count": total_count,
            "total_pages": total_pages,
            "databases_checked": dbs_checked,
            "current_page": page,
            "tv_shows": [convert_objectid_to_str(result) for result in results],
        }



    async def search_documents(
            self, 
            query: str, 
            page: int, 
            page_size: int
        ) -> dict:

            skip = (page - 1) * page_size
            
            words = query.split()
            regex_query = {
                '$regex': '.*' + '.*'.join(words) + '.*', 
                '$options': 'i'
            }
            
            tv_pipeline = [
                {"$match": {"$or": [
                    {"title": regex_query},
                    {"seasons.episodes.telegram.name": regex_query}
                ]}},
                {"$project": {
                    "_id": 1, "tmdb_id": 1, "title": 1, "genres": 1, "rating": 1, "imdb_id": 1,
                    "release_year": 1, "poster": 1, "backdrop": 1, "description": 1, "logo": 1,
                    "media_type": 1, "db_index": 1
                }}
            ]
            
            movie_pipeline = [
                {"$match": {"$or": [
                    {"title": regex_query},
                    {"telegram.name": regex_query}
                ]}},
                {"$project": {
                    "_id": 1, "tmdb_id": 1, "title": 1, "genres": 1, "rating": 1,
                    "release_year": 1, "poster": 1, "backdrop": 1, "description": 1,
                    "media_type": 1, "db_index": 1, "imdb_id": 1, "logo": 1
                }}
            ]
            
            results = []
            dbs_checked = []
            
            active_db_key = f"storage_{self.current_db_index}"
            active_db = self.dbs[active_db_key]
            dbs_checked.append(self.current_db_index)
            
            tv_results = await active_db["tv"].aggregate(tv_pipeline).to_list(None)
            movie_results = await active_db["movie"].aggregate(movie_pipeline).to_list(None)
            combined = tv_results + movie_results
            results.extend(combined)
            
            if len(results) < page_size:
                previous_db_index = self.current_db_index - 1
                while previous_db_index > 0 and len(results) < page_size:
                    prev_db_key = f"storage_{previous_db_index}"
                    prev_db = self.dbs[prev_db_key]
                    tv_results_prev = await prev_db["tv"].aggregate(tv_pipeline).to_list(None)
                    movie_results_prev = await prev_db["movie"].aggregate(movie_pipeline).to_list(None)
                    combined_prev = tv_results_prev + movie_results_prev
                    results.extend(combined_prev)
                    dbs_checked.append(previous_db_index)
                    previous_db_index -= 1

            total_count = 0
            for db_index in dbs_checked:
                key = f"storage_{db_index}"
                db = self.dbs[key]
                tv_count = await db["tv"].count_documents({
                    "$or": [
                        {"title": regex_query},
                        {"seasons.episodes.telegram.name": regex_query}
                    ]
                })
                movie_count = await db["movie"].count_documents({
                    "$or": [
                        {"title": regex_query},
                        {"telegram.name": regex_query}
                    ]
                })
                total_count += (tv_count + movie_count)
            
            paged_results = results[skip:skip + page_size]

            return {
                "total_count": total_count,
                "results": [convert_objectid_to_str(doc) for doc in paged_results]
            }


    async def get_media_details(
        self, tmdb_id: int, db_index: int,
        season_number: Optional[int] = None, episode_number: Optional[int] = None
    ) -> Optional[dict]:
        db_key = f"storage_{db_index}"
        if episode_number is not None and season_number is not None:
            tv_show = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
            if not tv_show:
                return None
            for season in tv_show.get("seasons", []):
                if season.get("season_number") == season_number:
                    for episode in season.get("episodes", []):
                        if episode.get("episode_number") == episode_number:
                            details = convert_objectid_to_str(episode)
                            details.update({
                                "tmdb_id": tmdb_id,
                                "type": "tv",
                                "season_number": season_number,
                                "episode_number": episode_number,
                                "backdrop": episode.get("episode_backdrop")
                            })
                            return details
            return None

        elif season_number is not None:
            tv_show = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
            if not tv_show:
                return None
            for season in tv_show.get("seasons", []):
                if season.get("season_number") == season_number:
                    details = convert_objectid_to_str(season)
                    details.update({
                        "tmdb_id": tmdb_id,
                        "type": "tv",
                        "season_number": season_number
                    })
                    return details
            return None

        else:
            tv_doc = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
            if tv_doc:
                tv_doc = convert_objectid_to_str(tv_doc)
                tv_doc["type"] = "tv"
                return tv_doc
            movie_doc = await self.dbs[db_key]["movie"].find_one({"tmdb_id": tmdb_id})
            if movie_doc:
                movie_doc = convert_objectid_to_str(movie_doc)
                movie_doc["type"] = "movie"
                return movie_doc
            return None


    # -------------------------------
    # DB Method for Edit Post
    # -------------------------------


    async def get_document(self, media_type: str, tmdb_id: int, db_index: int) -> Optional[Dict[str, Any]]:
        db_key = f"storage_{db_index}"
        if media_type.lower() in ["tv", "series"]:
            collection_name = "tv"
        else:
            collection_name = "movie"
        document = await self.dbs[db_key][collection_name].find_one({"tmdb_id": int(tmdb_id)})
        return convert_objectid_to_str(document) if document else None

    async def update_document(
        self, media_type: str, tmdb_id: int, db_index: int, update_data: Dict[str, Any]
    ):
        update_data.pop('_id', None)
        db_key = f"storage_{db_index}"
        if media_type.lower() in ["tv", "series"]:
            collection_name = "tv"
        else:
            collection_name = "movie"
        collection = self.dbs[db_key][collection_name]

        try:
            result = await collection.update_one({"tmdb_id": int(tmdb_id)}, {"$set": update_data})

            return result.modified_count > 0

        except Exception as e:
            err_str = str(e).lower()
            LOGGER.error(f"Error updating document in {db_key}: {e}")
            if "storage" in err_str or "quota" in err_str:
                total_storage_dbs = len(self.dbs) - 1
                db_index_int = int(db_index)
                next_db_index = (db_index_int % total_storage_dbs) + 1
                if next_db_index == 1:
                    LOGGER.warning("⚠️ All storage databases are full! Add more.")
                    return False

                new_db_key = f"storage_{next_db_index}"
                LOGGER.info(f"Switching from {db_key} to {new_db_key} due to storage error.")

                try:
                    old_doc = await self.dbs[db_key][collection_name].find_one({"tmdb_id": int(tmdb_id)})
                    if not old_doc:
                        LOGGER.error(f"Document with tmdb_id {tmdb_id} not found in {db_key} during migration.")
                        return False

                    old_doc.update(update_data)
                    old_doc["db_index"] = next_db_index
                    old_doc.pop("_id", None)
                    insert_result = await self.dbs[new_db_key][collection_name].insert_one(old_doc)
                    LOGGER.info(f"Inserted document {insert_result.inserted_id} into {new_db_key}")
                    await self.dbs[db_key][collection_name].delete_one({"tmdb_id": int(tmdb_id)})
                    LOGGER.info(f"Deleted document tmdb_id {tmdb_id} from {db_key}")
                    self.current_db_index = next_db_index
                    await self.update_current_db_index()
                    LOGGER.info(f"Switched to {new_db_key} and document migrated successfully.")
                    return True

                except Exception as migrate_error:
                    LOGGER.error(f"Error migrating document tmdb_id {tmdb_id} to {new_db_key}: {migrate_error}")
                    return False
            raise

    async def delete_document(self, media_type: str, tmdb_id: int, db_index: int) -> bool:
        db_key = f"storage_{db_index}"

        if media_type == "Movie":
            doc = await self.dbs[db_key]["movie"].find_one({"tmdb_id": tmdb_id})
            if doc and "telegram" in doc:
                for quality in doc["telegram"]:
                    try:
                        old_id = quality.get("id")
                        if old_id:
                            decoded_data = await decode_string(old_id)
                            chat_id = int(f"-100{decoded_data['chat_id']}")
                            msg_id = int(decoded_data['msg_id'])
                            create_task(delete_message(chat_id, msg_id))
                    except Exception as e:
                        LOGGER.error(f"Failed to queue file for deletion: {e}")
            
            result = await self.dbs[db_key]["movie"].delete_one({"tmdb_id": tmdb_id})
        else:
            doc = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
            if doc and "seasons" in doc:
                for season in doc["seasons"]:
                    for episode in season.get("episodes", []):
                        for quality in episode.get("telegram", []):
                            try:
                                old_id = quality.get("id")
                                if old_id:
                                    decoded_data = await decode_string(old_id)
                                    chat_id = int(f"-100{decoded_data['chat_id']}")
                                    msg_id = int(decoded_data['msg_id'])
                                    create_task(delete_message(chat_id, msg_id))
                            except Exception as e:
                                LOGGER.error(f"Failed to queue file for deletion: {e}")
            
            result = await self.dbs[db_key]["tv"].delete_one({"tmdb_id": tmdb_id})
        
        if result.deleted_count > 0:
            LOGGER.info(f"{media_type} with tmdb_id {tmdb_id} deleted successfully.")
            return True
        LOGGER.info(f"No document found with tmdb_id {tmdb_id}.")
        return False

    async def delete_movie_quality(self, tmdb_id: int, db_index: int, quality: str) -> bool:
        db_key = f"storage_{db_index}"
        movie = await self.dbs[db_key]["movie"].find_one({"tmdb_id": tmdb_id})
        
        if not movie or "telegram" not in movie:
            return False

        for q in movie["telegram"]:
            if q.get("quality") == quality:
                try:
                    old_id = q.get("id")
                    if old_id:
                        decoded_data = await decode_string(old_id)
                        chat_id = int(f"-100{decoded_data['chat_id']}")
                        msg_id = int(decoded_data['msg_id'])
                        create_task(delete_message(chat_id, msg_id))
                except Exception as e:
                    LOGGER.error(f"Failed to queue file for deletion: {e}")
                break
        
        original_len = len(movie["telegram"])
        movie["telegram"] = [q for q in movie["telegram"] if q.get("quality") != quality]
        
        if len(movie["telegram"]) == original_len:
            return False
        
        movie['updated_on'] = datetime.utcnow()
        result = await self.dbs[db_key]["movie"].replace_one({"tmdb_id": tmdb_id}, movie)
        return result.modified_count > 0

    async def delete_tv_episode(self, tmdb_id: int, db_index: int, season_number: int, episode_number: int) -> bool:
        db_key = f"storage_{db_index}"
        tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
        
        if not tv or "seasons" not in tv:
            return False
        
        found = False
        for season in tv["seasons"]:
            if season.get("season_number") == season_number:
                for ep in season["episodes"]:
                    if ep.get("episode_number") == episode_number:
                        for quality in ep.get("telegram", []):
                            try:
                                old_id = quality.get("id")
                                if old_id:
                                    decoded_data = await decode_string(old_id)
                                    chat_id = int(f"-100{decoded_data['chat_id']}")
                                    msg_id = int(decoded_data['msg_id'])
                                    create_task(delete_message(chat_id, msg_id))
                            except Exception as e:
                                LOGGER.error(f"Failed to queue file for deletion: {e}")
                        break
                
                original_len = len(season["episodes"])
                season["episodes"] = [ep for ep in season["episodes"] if ep.get("episode_number") != episode_number]
                found = original_len > len(season["episodes"])
                break
        
        if not found:
            return False
        
        tv['updated_on'] = datetime.utcnow()
        result = await self.dbs[db_key]["tv"].replace_one({"tmdb_id": tmdb_id}, tv)
        return result.modified_count > 0

    async def delete_tv_season(self, tmdb_id: int, db_index: int, season_number: int) -> bool:
        db_key = f"storage_{db_index}"
        tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
        
        if not tv or "seasons" not in tv:
            return False
        
        for season in tv["seasons"]:
            if season.get("season_number") == season_number:
                for episode in season.get("episodes", []):
                    for quality in episode.get("telegram", []):
                        try:
                            old_id = quality.get("id")
                            if old_id:
                                decoded_data = await decode_string(old_id)
                                chat_id = int(f"-100{decoded_data['chat_id']}")
                                msg_id = int(decoded_data['msg_id'])
                                create_task(delete_message(chat_id, msg_id))
                        except Exception as e:
                            LOGGER.error(f"Failed to queue file for deletion: {e}")
            
            result = await self.dbs[db_key]["tv"].delete_one({"tmdb_id": tmdb_id})
        
        if result.deleted_count > 0:
            LOGGER.info(f"Deleted season {season_number} of TV show {tmdb_id}.")
            return True
        return False

    # -------------------------------
    # API Token Methods
    # -------------------------------

    async def add_api_token(self, name: str, daily_limit_gb: float = None, monthly_limit_gb: float = None) -> dict:
        """Generates a random alphanumeric token (min 20 chars) and saves it."""
        alphabet = string.ascii_letters + string.digits
        token = ''.join(secrets.choice(alphabet) for _ in range(32))  # 32 chars > 20 chars
        
        token_doc = {
            "name": name,
            "token": token,
            "created_at": datetime.utcnow(),
            "limits": {
                "daily_limit_gb": daily_limit_gb if daily_limit_gb else 0,
                "monthly_limit_gb": monthly_limit_gb if monthly_limit_gb else 0
            },
            "usage": {
                "total_bytes": 0,
                "daily": {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "bytes": 0},
                "monthly": {"month": datetime.now(timezone.utc).strftime("%Y-%m"), "bytes": 0}
            }
        }
        
        await self.dbs["tracking"]["api_tokens"].insert_one(token_doc)
        return convert_objectid_to_str(token_doc)

    async def get_api_token(self, token: str) -> Optional[dict]:
        """Retrieves token data."""
        doc = await self.dbs["tracking"]["api_tokens"].find_one({"token": token})
        return convert_objectid_to_str(doc) if doc else None

    async def get_all_api_tokens(self) -> List[dict]:
        """Lists all tokens."""
        cursor = self.dbs["tracking"]["api_tokens"].find().sort("created_at", DESCENDING)
        tokens = await cursor.to_list(None)
        return [convert_objectid_to_str(token) for token in tokens]

    async def revoke_api_token(self, token: str) -> bool:
        """Deletes a token."""
        result = await self.dbs["tracking"]["api_tokens"].delete_one({"token": token})
        return result.deleted_count > 0

    async def update_token_usage(self, token: str, bytes_delta: int):
        """Atomically updates token usage statistics, handling day/month rollovers."""
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        month_str = datetime.now(timezone.utc).strftime("%Y-%m")
        
        token_doc = await self.dbs["tracking"]["api_tokens"].find_one({"token": token})
        if not token_doc:
             return

        # Check for rollovers
        current_daily = token_doc.get("usage", {}).get("daily", {})
        if current_daily.get("date") != today_str:
            # Reset daily
            await self.dbs["tracking"]["api_tokens"].update_one(
                {"token": token},
                {"$set": {"usage.daily": {"date": today_str, "bytes": 0}}}
            )

        current_monthly = token_doc.get("usage", {}).get("monthly", {})
        if current_monthly.get("month") != month_str:
            # Reset monthly
            await self.dbs["tracking"]["api_tokens"].update_one(
                {"token": token},
                {"$set": {"usage.monthly": {"month": month_str, "bytes": 0}}}
            )

        # Increment usage
        await self.dbs["tracking"]["api_tokens"].update_one(
            {"token": token},
            {
                "$inc": {
                    "usage.total_bytes": bytes_delta,
                    "usage.daily.bytes": bytes_delta,
                    "usage.monthly.bytes": bytes_delta
                }
            }
        )
    
    async def reset_api_usage_stats(self):
        """
        Background task to reset daily and monthly usage stats if the date has changed.
        This runs proactively to ensure Dashboard shows 0 usage even if no new traffic has occurred.
        """
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        month_str = datetime.now(timezone.utc).strftime("%Y-%m")
        
        # 1. Reset Daily Usage for any token where date != today
        await self.dbs["tracking"]["api_tokens"].update_many(
            {"usage.daily.date": {"$ne": today_str}},
            {"$set": {"usage.daily": {"date": today_str, "bytes": 0}}}
        )
        
        # 2. Reset Monthly Usage for any token where month != current_month
        await self.dbs["tracking"]["api_tokens"].update_many(
            {"usage.monthly.month": {"$ne": month_str}},
            {"$set": {"usage.monthly": {"month": month_str, "bytes": 0}}}
        )

    async def update_api_token_limits(self, token: str, daily_limit_gb: float, monthly_limit_gb: float) -> bool:
        """Updates the bandwidth limits for an existing token."""
        result = await self.dbs["tracking"]["api_tokens"].update_one(
            {"token": token},
            {"$set": {
                "limits": {
                    "daily_limit_gb": daily_limit_gb if daily_limit_gb else 0,
                    "monthly_limit_gb": monthly_limit_gb if monthly_limit_gb else 0
                }
            }}
        )
        return result.modified_count > 0

    async def delete_tv_quality(self, tmdb_id: int, db_index: int, season_number: int, episode_number: int, quality: str) -> bool:
        db_key = f"storage_{db_index}"
        tv = await self.dbs[db_key]["tv"].find_one({"tmdb_id": tmdb_id})
        
        if not tv or "seasons" not in tv:
            return False
        
        found = False
        for season in tv["seasons"]:
            if season.get("season_number") == season_number:
                for episode in season["episodes"]:
                    if episode.get("episode_number") == episode_number and "telegram" in episode:
                        for q in episode["telegram"]:
                            if q.get("quality") == quality:
                                try:
                                    old_id = q.get("id")
                                    if old_id:
                                        decoded_data = await decode_string(old_id)
                                        chat_id = int(f"-100{decoded_data['chat_id']}")
                                        msg_id = int(decoded_data['msg_id'])
                                        create_task(delete_message(chat_id, msg_id))
                                except Exception as e:
                                    LOGGER.error(f"Failed to queue file for deletion: {e}")
                                break
                        
                        original_len = len(episode["telegram"])
                        episode["telegram"] = [q for q in episode["telegram"] if q.get("quality") != quality]
                        found = original_len > len(episode["telegram"])
                        break
        
        if not found:
            return False
        tv['updated_on'] = datetime.utcnow()
        result = await self.dbs[db_key]["tv"].replace_one({"tmdb_id": tmdb_id}, tv)
        return result.modified_count > 0


    # Get per-DB statistics (movies, tv shows, used size, etc.)
    async def get_database_stats(self):
        stats = []
        for key in self.dbs.keys():
            if key.startswith("storage_"):
                try:
                    db = self.dbs[key]
                    movie_count = await db["movie"].count_documents({})
                    tv_count = await db["tv"].count_documents({})
                    
                    try:
                        db_stats = await db.command("dbstats")
                    except Exception as e:
                        LOGGER.warning(f"Failed to get dbstats for {key}: {e}")
                        db_stats = {}

                    stats.append({
                        "db_name": key,
                        "movie_count": movie_count,
                        "tv_count": tv_count,
                        "storageSize": db_stats.get("storageSize", 0),
                        "dataSize": db_stats.get("dataSize", 0)
                    })
                except Exception as e:
                     LOGGER.error(f"Error fetching stats for {key}: {e}")
        return stats

    # ---------------------------
    # MANAGED UPDATES Implementation
    # ---------------------------

    async def get_pending_updates(self, page: int = 1, page_size: int = 20):
        # We need to group by the "conflict slot"
        skip = (page - 1) * page_size
        collection = self.dbs["tracking"]["pending_updates"]
        
        pipeline = [
            # 1. Group by slot
            {
                "$group": {
                    "_id": {
                        "tmdb_id": "$tmdb_id",
                        "media_type": "$media_type",
                        "quality": "$quality",
                        "season": "$season",   # will be null for movies
                        "episode": "$episode"  # will be null for movies
                    },
                    "doc_id": {"$first": "$_id"}, # Keep one ID for reference/sorting
                    "metadata": {"$first": "$metadata"}, # Keep common metadata
                    "created_at": {"$max": "$created_at"}, # Use latest creation? Or first?
                    "candidates": {
                        "$push": {
                            "_id": "$_id",
                            "new_file": "$new_file",
                            "created_at": "$created_at"
                        }
                    }
                }
            },
            # 2. Sort by most recent
            {"$sort": {"created_at": 1}},
            # 3. Facet for pagination
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "data": [{"$skip": skip}, {"$limit": page_size}]
                }
            }
        ]
        
        # We need to be careful with "Auto-resolve" logic from previous code.
        # The previous code iterated and checked for "Active File". If missing -> Auto-resolve.
        # We can do this AFTER fetching the page.
        
        try:
            agg_result = await collection.aggregate(pipeline).to_list(None)
        except Exception as e:
            LOGGER.error(f"Aggregation failed: {e}")
            return [], 0

        if not agg_result:
            return [], 0
            
        result_data = agg_result[0]["data"]
        total = agg_result[0]["metadata"][0]["total"] if agg_result[0]["metadata"] else 0
        
        enriched = []
        
        for item in result_data:
            # Reconstruct a flat-ish object but with candidates
            # The UI expects top-level fields: media_type, quality, season, etc.
            group_key = item["_id"]
            
            doc = {
                "_id": str(item["doc_id"]), # Representative ID (mostly for unique key in UI loops)
                "tmdb_id": group_key["tmdb_id"],
                "media_type": group_key["media_type"],
                "quality": group_key["quality"],
                "season": group_key.get("season"),
                "episode": group_key.get("episode"),
                "metadata": item["metadata"],
                "candidates": []
            }
            
            # Fetch active info ONCE for this group
            # We can use the first candidate to convert to objectid format if needed, 
            # but get_active_file_info uses dict.
            
            # Helper needs a dict with tmdb_id, media_type, etc.
            # We can pass `doc` itself as it has the keys.
            old_file = await self._get_active_file_info_internal(doc)
            doc["old_file"] = old_file
            
            # Auto-resolve checks
            if old_file is None:
                # If no current file, auto-accept the BEST/FIRST candidate?
                # Previous logic: "accepting new file" (first one encountered).
                # We will take the first candidate from the list.
                candidate_to_accept = item["candidates"][0]
                LOGGER.info(f"Auto-resolving pending update group {group_key}: Current file not found, accepting first candidate.")
                await self.resolve_pending_update(str(candidate_to_accept["_id"]), "keep_new")
                # Do not add to enriched, as it's resolved.
                # Note: This messes up page size count slightly (returns fewer than 20), but that's fine for now.
                continue
                
            # Double Extension Auto-Fix logic (check all candidates?)
            # Previous logic checked 1-to-1.
            # Here we might have multiple. We should filter candidates?
            # If a candidate is just a rename of old_file, we might auto-resolve IT.
            # But wait, "resolve" with "keep_new" deletes others.
            # If we have 3 candidates, and Candidate A is "rename fix", 
            # if we accept A, B and C are deleted. Is that desired? 
            # Probably yes, B and C are duplicates too.
            
            final_candidates = []
            group_resolved = False
            
            old_name = old_file.get("file_name", "") or old_file.get("name", "")
            old_size = old_file.get("file_size", 0) or old_file.get("size", 0)
            
            for cand in item["candidates"]:
                cand_doc = convert_objectid_to_str(cand)
                new_file = cand_doc["new_file"]
                new_name = new_file.get("file_name", "") or new_file.get("name", "")
                new_size = new_file.get("file_size", 0) or new_file.get("size", 0)
                
                # Check double extension fix
                if old_name.endswith(".mkv.mkv") and new_name == old_name[:-4] and old_size == new_size:
                     LOGGER.info(f"Auto-resolving double extension fix {cand_doc['_id']}: Replacing {old_name} with {new_name}")
                     await self.resolve_pending_update(str(cand_doc["_id"]), "keep_new")
                     group_resolved = True
                     break
                
                final_candidates.append(cand_doc)
            
            if group_resolved:
                continue
                
            doc["candidates"] = final_candidates
            enriched.append(doc)

        return enriched, total

    async def _get_active_file_info_internal(self, info_dict: dict):
        # Renamed from get_active_file_info to avoid confusion, internal usage
        tmdb_id = info_dict.get("tmdb_id")
        media_type = info_dict.get("media_type")
        quality = info_dict.get("quality")
        
        total_storage_dbs = len(self.dbs) - 1
        found_doc = None
        
        for i in range(1, total_storage_dbs + 1):
            db_key = f"storage_{i}"
            col_name = "movie" if media_type == "movie" else "tv"
            found_doc = await self.dbs[db_key][col_name].find_one({"tmdb_id": tmdb_id})
            if found_doc:
                break
        
        if not found_doc:
            return None
            
        # Extract quality info
        updated_db = False
        target_q = None

        if media_type == "movie":
            telegram_files = found_doc.get("telegram", [])
            for q in telegram_files:
                if q.get("quality") == quality:
                    target_q = q
                    # Check if missing info
                    if not q.get("dc_id") or not q.get("file_type") or str(q.get("file_type")).lower() == "unknown":
                        dc, ftype = await self._fetch_telegram_file_info(q.get("id"))
                        if dc or ftype:
                            if dc: q["dc_id"] = dc
                            if ftype: q["file_type"] = ftype
                            updated_db = True
                    break # Found the quality we wanted
            
            if updated_db:
                try:
                    await self.dbs[db_key]["movie"].update_one(
                        {"_id": found_doc["_id"]},
                        {"$set": {"telegram": telegram_files}}
                    )
                    LOGGER.info(f"Backfilled metadata for movie {found_doc.get('title')} ({quality})")
                except Exception as e:
                    LOGGER.error(f"Failed to backfill metadata: {e}")
                    
            return target_q

        else: # TV
            sn = info_dict.get("season")
            en = info_dict.get("episode")
            seasons = found_doc.get("seasons", [])
            
            for season in seasons:
                if season.get("season_number") == sn:
                    for episode in season.get("episodes", []):
                        if episode.get("episode_number") == en:
                            telegram_files = episode.get("telegram", [])
                            for q in telegram_files:
                                if q.get("quality") == quality:
                                    target_q = q
                                    # Check missing info
                                    if not q.get("dc_id") or not q.get("file_type") or str(q.get("file_type")).lower() == "unknown":
                                        dc, ftype = await self._fetch_telegram_file_info(q.get("id"))
                                        if dc or ftype:
                                            if dc: q["dc_id"] = dc
                                            if ftype: q["file_type"] = ftype
                                            updated_db = True
                                    break
                            break
                    break
            
            if updated_db:
                try:
                    await self.dbs[db_key]["tv"].update_one(
                        {"_id": found_doc["_id"]},
                        {"$set": {"seasons": seasons}}
                    )
                    LOGGER.info(f"Backfilled metadata for TV {found_doc.get('title')} S{sn}E{en} ({quality})")
                except Exception as e:
                    LOGGER.error(f"Failed to backfill metadata TV: {e}")

            return target_q
        
        return None

    async def resolve_pending_update(self, pending_id_str: str, decision: str):
        collection = self.dbs["tracking"]["pending_updates"]
        pending_doc = await collection.find_one({"_id": ObjectId(pending_id_str)})
        
        if not pending_doc:
            return False, "Pending update not found"

    async def delete_pending_update_and_files(self, pending_id_str: str):
        collection = self.dbs["tracking"]["pending_updates"]
        pending_doc = await collection.find_one({"_id": ObjectId(pending_id_str)})
        
        if not pending_doc:
            return False, "Pending update not found", []

        # Identify slot
        tmdb_id = pending_doc["tmdb_id"]
        media_type = pending_doc["media_type"]
        quality = pending_doc["quality"]
        
        query = {
            "tmdb_id": tmdb_id,
            "media_type": media_type,
            "quality": quality
        }
        
        season = None
        episode = None
        if media_type == "tv":
            season = pending_doc["season"]
            episode = pending_doc["episode"]
            query["season"] = season
            query["episode"] = episode

        deletion_list = [] # List of (chat_id, msg_id) tuples

        # 1. DELETE CANDIDATES (Pending Files)
        # Find ALL pending items for this slot
        slot_pendings = await collection.find(query).to_list(None)
        
        for p in slot_pendings:
            try:
                f_info = p.get("new_file")
                if f_info and "id" in f_info:
                    decoded = await decode_string(f_info["id"])
                    chat_id = int(f"-100{decoded['chat_id']}")
                    msg_id = int(decoded['msg_id'])
                    deletion_list.append((chat_id, msg_id))
            except Exception as e:
                LOGGER.error(f"Failed to parse candidate file info for deletion: {e}")
        
        # Remove all candidates from Pending DB
        await collection.delete_many(query)

        # 2. DELETE CURRENT FILE (Main DB)
        current_db_key = f"storage_{self.current_db_index}" # Note: We should check all storage DBs
        total_storage_dbs = len(self.dbs) - 1
        
        found_db_key = None
        main_doc = None
        
        # Find the main document
        for i in range(1, total_storage_dbs + 1):
            key = f"storage_{i}"
            if media_type == "movie":
                main_doc = await self.dbs[key]["movie"].find_one({"tmdb_id": tmdb_id})
            else:
                main_doc = await self.dbs[key]["tv"].find_one({"tmdb_id": tmdb_id})
                
            if main_doc:
                found_db_key = key
                break
        
        if main_doc:
            file_removed_from_db = False
            
            if media_type == "movie":
                existing_qualities = main_doc.get("telegram", [])
                target_q = next((q for q in existing_qualities if q["quality"] == quality), None)
                
                if target_q:
                    # Add to deletion list
                    try:
                        if "id" in target_q:
                            decoded = await decode_string(target_q["id"])
                            chat_id = int(f"-100{decoded['chat_id']}")
                            msg_id = int(decoded['msg_id'])
                            deletion_list.append((chat_id, msg_id))
                    except Exception as e:
                        LOGGER.error(f"Failed to parse current file info for deletion: {e}")
                    
                    # Remove from DB list
                    updated_qualities = [q for q in existing_qualities if q["quality"] != quality]
                    if updated_qualities:
                        await self.dbs[found_db_key]["movie"].update_one(
                            {"_id": main_doc["_id"]},
                            {"$set": {"telegram": updated_qualities, "updated_on": datetime.utcnow()}}
                        )
                    else:
                         # Option: Delete movie if no qualities left? 
                         # For now, let's just leave it empty or remove the telegram field.
                         # User request: "Delete current file". Usually implies removing the file link.
                         await self.dbs[found_db_key]["movie"].update_one(
                            {"_id": main_doc["_id"]},
                            {"$set": {"telegram": [], "updated_on": datetime.utcnow()}}
                        )
                    file_removed_from_db = True

            else: # TV Show
                # Locate season/episode
                seasons = main_doc.get("seasons", [])
                for s in seasons:
                    if s["season_number"] == season:
                        for e in s["episodes"]:
                            if e["episode_number"] == episode:
                                existing_qualities = e.get("telegram", [])
                                target_q = next((q for q in existing_qualities if q["quality"] == quality), None)
                                
                                if target_q:
                                    # Add to deletion list
                                    try:
                                        if "id" in target_q:
                                            decoded = await decode_string(target_q["id"])
                                            chat_id = int(f"-100{decoded['chat_id']}")
                                            msg_id = int(decoded['msg_id'])
                                            deletion_list.append((chat_id, msg_id))
                                    except Exception as e:
                                        LOGGER.error(f"Failed to parse current TV file info for deletion: {e}")

                                    # Remove from DB list
                                    e["telegram"] = [q for q in existing_qualities if q["quality"] != quality]
                                    file_removed_from_db = True
                                break
                        break
                
                if file_removed_from_db:
                    await self.dbs[found_db_key]["tv"].replace_one({"_id": main_doc["_id"]}, main_doc)

            if file_removed_from_db:
                LOGGER.info(f"Removed current file from Main DB for {tmdb_id} ({quality})")

        return True, "Deleted all files (current and candidates) from Database and Telegram.", deletion_list

    async def resolve_pending_update(self, pending_id_str: str, decision: str):
        collection = self.dbs["tracking"]["pending_updates"]
        pending_doc = await collection.find_one({"_id": ObjectId(pending_id_str)})
        
        if not pending_doc:
            return False, "Pending update not found", []

        # Identify slot
        query = {
            "tmdb_id": pending_doc["tmdb_id"],
            "media_type": pending_doc["media_type"],
            "quality": pending_doc["quality"]
        }
        if pending_doc["media_type"] == "tv":
            query["season"] = pending_doc["season"]
            query["episode"] = pending_doc["episode"]

        # Find ALL pending items for this slot
        slot_pendings = await collection.find(query).to_list(None)
        
        deletion_list = [] # List of (chat_id, msg_id) tuples

        if decision == "keep_old":
            # REJECT ALL
            for p in slot_pendings:
                try:
                    f_info = p.get("new_file")
                    if f_info and "id" in f_info:
                        decoded = await decode_string(f_info["id"])
                        chat_id = int(f"-100{decoded['chat_id']}")
                        msg_id = int(decoded['msg_id'])
                        deletion_list.append((chat_id, msg_id))
                except Exception as e:
                    LOGGER.error(f"Failed to parse file info for deletion: {e}")
                
            # Remove all from DB
            await collection.delete_many(query)
            return True, "Rejected all pending updates for this slot.", deletion_list

        elif decision == "keep_new":
            # ACCEPT "pending_doc" AS WINNER
            winner = pending_doc
            
            # 1. Promote Winner
            try:
                if winner["media_type"] == "movie":
                    meta = winner["metadata"] 
                    
                    winner_file = winner["new_file"]
                    winner_quality = QualityDetail(
                        quality=winner["quality"],
                        id=winner_file["id"],
                        name=winner_file["name"], 
                        size=winner_file["size"],
                        dc_id=winner_file.get("dc_id"),
                        file_type=winner_file.get("file_type", "video")
                    )
                    
                    movie_schema = MovieSchema(
                        tmdb_id=winner["tmdb_id"],
                        title=winner["metadata"]["title"],
                        release_year=winner["metadata"]["year"],
                        media_type="movie",
                        db_index=1, # Dummy, will be resolved
                        telegram=[winner_quality]
                    )
                    
                    await self.update_movie(movie_schema, force_update=True)
                
                else: 
                    # TV Show
                    meta = winner["metadata"]
                    sn = winner["season"]
                    en = winner["episode"]
                    winner_file = winner["new_file"]
                    
                    # Create proper QualityDetail object for TV
                    winner_quality_tv = QualityDetail(
                        quality=winner["quality"],
                        id=winner_file["id"],
                        name=winner_file["name"],
                        size=winner_file["size"],
                        dc_id=winner_file.get("dc_id"),
                        file_type=winner_file.get("file_type", "video")
                    )
                    
                    episode_schema = Episode(
                        episode_number=en,
                        title=meta.get("episode_title", f"Episode {en}"),
                        telegram=[winner_quality_tv]
                    )
                    season_schema = Season(
                        season_number=sn,
                        episodes=[episode_schema]
                    )
                    tv_schema = TVShowSchema(
                        tmdb_id=winner["tmdb_id"],
                        title=meta["title"],
                        release_year=meta["year"],
                        media_type="tv",
                        db_index=1,
                        seasons=[season_schema]
                    )
                     
                    await self.update_tv_show(tv_schema, force_update=True)

            except Exception as e:
                LOGGER.error(f"Failed to promote winner: {e}")
                return False, f"Failed to promote: {str(e)}", []

            # 2. Delete LOSERS (All OTHER pending files)
            for p in slot_pendings:
                if p["_id"] == winner["_id"]:
                    continue # Skip winner
                
                # Loose files need to be deleted
                try:
                    f_info = p.get("new_file")
                    if f_info and "id" in f_info:
                        decoded = await decode_string(f_info["id"])
                        chat_id = int(f"-100{decoded['chat_id']}")
                        msg_id = int(decoded['msg_id'])
                        deletion_list.append((chat_id, msg_id))
                except Exception as e:
                    LOGGER.error(f"Failed to parse loser pending file: {e}")

            # 3. Clear Pending
            await collection.delete_many(query)
            return True, "Winner promoted and duplicates removed.", deletion_list
            
        return False, "Invalid decision", []

    async def reidentify_pending_update(self, pending_id_str: str, new_metadata: dict):
        collection = self.dbs["tracking"]["pending_updates"]
        pending_doc = await collection.find_one({"_id": ObjectId(pending_id_str)})
        
        if not pending_doc:
            return False, "Pending document not found"
            
        try:
            # Construct Schema from new metadata + existing file info
            file_info = pending_doc["new_file"]
            
            # Helper to convert dictionary 'new_file' back to QualityDetail or whatever update_movie expects
            # In insert_media/update_movie:
            # Input is QualityDetail (dict in pydantic).
            # new_file stored in pending_doc IS the QualityDetail dict.
            
            # We need to construct the full Schema
            result = None
            if new_metadata["media_type"] == "movie":
                # Ensure quality list structure
                quality_detail = QualityDetail(
                    quality=pending_doc["quality"], # Or use file_info["quality"]
                    id=file_info["id"],
                    name=file_info["name"],
                    size=file_info["size"],
                    dc_id=file_info.get("dc_id"),
                    file_type=file_info.get("file_type", "video")
                )
                
                movie_schema = MovieSchema(
                    tmdb_id=new_metadata["tmdb_id"],
                    imdb_id=new_metadata.get("imdb_id"),
                    db_index=self.current_db_index,
                    title=new_metadata["title"],
                    genres=new_metadata.get("genres"),
                    description=new_metadata.get("description"),
                    rating=new_metadata.get("rate"),
                    release_year=new_metadata.get("year"),
                    poster=new_metadata.get("poster"),
                    backdrop=new_metadata.get("backdrop"),
                    logo=new_metadata.get("logo"),
                    cast=new_metadata.get("cast"),
                    runtime=new_metadata.get("runtime"),
                    media_type="movie",
                    telegram=[quality_detail]
                )
                
                # Check duplicates is default behavior (force_update=False)
                # If it conflicts with the CORRECT movie, it will return "PENDING" and create a NEW pending doc.
                # If it is unique, it will return ID.
                result = await self.update_movie(movie_schema)
                
            else:
                # TV Show logic
                quality_detail = QualityDetail(
                    quality=pending_doc["quality"],
                    id=file_info["id"],
                    name=file_info["name"],
                    size=file_info["size"],
                    dc_id=file_info.get("dc_id"),
                    file_type=file_info.get("file_type", "video")
                )
                
                ep_num = pending_doc["episode"]
                sn_num = pending_doc["season"]
                
                # We need episode details from new_metadata if available or minimal
                # metadata helper returns 'episode_title', 'episode_overview' etc for TV
                
                episode_schema = Episode(
                    episode_number=ep_num,
                    title=new_metadata.get("episode_title", f"Episode {ep_num}"),
                    episode_backdrop=new_metadata.get("episode_backdrop"),
                    overview=new_metadata.get("episode_overview"),
                    released=new_metadata.get("episode_released"),
                    telegram=[quality_detail]
                )
                
                season_schema = Season(
                    season_number=sn_num,
                    episodes=[episode_schema]
                )
                
                tv_schema = TVShowSchema(
                    tmdb_id=new_metadata["tmdb_id"],
                    imdb_id=new_metadata.get("imdb_id"),
                    db_index=self.current_db_index,
                    title=new_metadata["title"],
                    genres=new_metadata.get("genres"),
                    description=new_metadata.get("description"),
                    rating=new_metadata.get("rate"),
                    release_year=new_metadata.get("year"),
                    poster=new_metadata.get("poster"),
                    backdrop=new_metadata.get("backdrop"),
                    logo=new_metadata.get("logo"),
                    cast=new_metadata.get("cast"),
                    runtime=new_metadata.get("runtime"),
                    media_type="tv",
                    seasons=[season_schema]
                )
                
                result = await self.update_tv_show(tv_schema)

            # Cleanup
            if result:
                # If result is "PENDING" -> New pending doc created. Delete old one.
                # If result is ObjectId -> Success. Delete old one.
                await collection.delete_one({"_id": ObjectId(pending_id_str)})
                
                if result == "PENDING":
                    return True, "Match updated, but file is still a duplicate (queued in pending)."
                return True, "File successfully moved to library!"
            
            return False, "Failed to update media."

        except Exception as e:
            LOGGER.error(f"Reidentify failed: {e}")
            return False, str(e)

    async def bulk_resolve_pending_updates(self, selections: list):
        """
        Process multiple pending update resolutions with round-robin bot deletion.
        
        Args:
            selections: List of {"pending_id": str, "action": str} dicts
        """
        import asyncio
        from Backend.logger import LOGGER
        from Backend.helper.task_manager import delete_multiple_messages
        
        total = len(selections)
        processed = 0
        failed = []
        all_deletions = []
        
        LOGGER.info(f"Starting bulk resolve for {total} pending updates")
        
        # Phase 1: DB Updates
        for selection in selections:
            pending_id = selection.get("pending_id")
            action = selection.get("action")
            
            try:
                # Resolve individual update (DB only)
                success, msg, local_deletions = await self.resolve_pending_update(pending_id, action)
                
                if not success:
                    failed.append({"id": pending_id, "reason": msg})
                    LOGGER.warning(f"Failed to resolve {pending_id}: {msg}")
                else:
                    LOGGER.info(f"✓ Resolved {pending_id} (DB Updated)")
                    if local_deletions:
                        all_deletions.extend(local_deletions)
                
                processed += 1
                
            except Exception as e:
                failed.append({"id": pending_id, "reason": str(e)})
                LOGGER.error(f"Error processing {pending_id}: {e}")
                processed += 1
        
        # Phase 2: Bulk Deletion (Parallel)
        if all_deletions:
             LOGGER.info(f"Starting parallel deletion of {len(all_deletions)} files across bots...")
             await delete_multiple_messages(all_deletions)
             LOGGER.info("Deletion phase completed.")
        
        LOGGER.info(f"Bulk resolve completed: {processed}/{total} processed, {len(failed)} failed")
        
        return {
            "total": total,
            "processed": processed,
            "failed": failed
        }

    async def run_bulk_resolve_background(self, task_id: str, selections: list):
        """
        Background wrapper for bulk resolve to track progress.
        """
        import asyncio
        from Backend.logger import LOGGER
        from Backend.helper.task_manager import delete_multiple_messages
        
        total = len(selections)
        self.active_tasks[task_id] = {
            "status": "processing",
            "total": total,
            "processed": 0,
            "details": "Starting...",
            "failed": []
        }
        
        failed = []
        all_deletions = []
        
        LOGGER.info(f"Starting bulk resolve task {task_id} for {total} items")
        
        # Phase 1: DB Updates
        for i, selection in enumerate(selections):
            pending_id = selection.get("pending_id")
            action = selection.get("action")
            
            try:
                # Update status
                self.active_tasks[task_id]["details"] = f"Processing bundle {i+1} of {total}"
                
                # Resolve individual update
                success, msg, local_deletions = await self.resolve_pending_update(pending_id, action)
                
                if not success:
                    failed.append({"id": pending_id, "reason": msg})
                else:
                    if local_deletions:
                        all_deletions.extend(local_deletions)
                
                self.active_tasks[task_id]["processed"] = i + 1
                
            except Exception as e:
                LOGGER.error(f"Task {task_id} error on {pending_id}: {e}")
                failed.append({"id": pending_id, "reason": str(e)})
                self.active_tasks[task_id]["processed"] = i + 1
                
        # Phase 2: Bulk Deletion (Parallel)
        if all_deletions:
             self.active_tasks[task_id]["details"] = "Cleaning up files from Telegram..."
             LOGGER.info(f"Task {task_id}: Deleting {len(all_deletions)} files...")
             await delete_multiple_messages(all_deletions)
        
        # Completion
        self.active_tasks[task_id]["status"] = "completed"
        self.active_tasks[task_id]["details"] = "All operations completed."
        self.active_tasks[task_id]["failed"] = failed
        
        # Cleanup task data after some time? 
        # For now we rely on memory, restart clears it. Or we can have a reaper.
        # But this is fine for session based polling.
        
    def get_task_status(self, task_id: str):
        return self.active_tasks.get(task_id)

    async def clean_expired_pending_updates(self):
        """
        Remove pending updates older than 30 days.
        Runs as a background task every 24 hours.
        """
        from datetime import datetime, timedelta
        import asyncio
        from Backend.logger import LOGGER
        from Backend.helper.task_manager import delete_message
        
        while True:
            try:
                expiry_date = datetime.utcnow() - timedelta(days=30)
                collection = self.dbs["tracking"]["pending_updates"]
                
                # Find expired items
                expired_items = await collection.find({
                    "created_at": {"$lt": expiry_date}
                }).to_list(None)
                
                if expired_items:
                    LOGGER.info(f"Found {len(expired_items)} expired pending updates to cleanup")
                    
                    # Delete files from Telegram before removing from DB
                    deleted_count = 0
                    for item in expired_items:
                        try:
                            # We need to delete the NEW candidate file that was pending
                            f_info = item.get("new_file")
                            if f_info and "id" in f_info:
                                decoded = await decode_string(f_info["id"])
                                await delete_message(
                                    int(f"-100{decoded['chat_id']}"), 
                                    int(decoded['msg_id'])
                                )
                            deleted_count += 1
                        except Exception as e:
                            LOGGER.error(f"Failed to delete expired file for pending update {item.get('_id')}: {e}")
                    
                    # Remove from database
                    result = await collection.delete_many({
                        "created_at": {"$lt": expiry_date}
                    })
                    
                    LOGGER.info(f"Cleaned up {result.deleted_count} expired pending updates (Files deleted: {deleted_count})")
            
            except Exception as e:
                 LOGGER.error(f"Error in clean_expired_pending_updates task: {e}")
                 
            # Wait 24 hours before next check
            await asyncio.sleep(86400)

    async def search_pending_updates(self, query: str, page: int = 1, page_size: int = 20):
        """
        Search for pending updates by title (case-insensitive).
        """
        skip = (page - 1) * page_size
        collection = self.dbs["tracking"]["pending_updates"]

        # Case-insensitive search in metadata title
        search_query = {
            "metadata.title": {
                "$regex": query,
                "$options": "i"  # Case insensitive
            }
        }

        pipeline = [
            {"$match": search_query},
            # 1. Group by slot (similar to get_pending_updates)
            {
                "$group": {
                    "_id": {
                        "tmdb_id": "$tmdb_id",
                        "media_type": "$media_type",
                        "quality": "$quality",
                        "season": "$season",   # will be null for movies
                        "episode": "$episode"  # will be null for movies
                    },
                    "doc_id": {"$first": "$_id"}, # Keep one ID for reference/sorting
                    "metadata": {"$first": "$metadata"}, # Keep common metadata
                    "created_at": {"$max": "$created_at"}, # Use latest creation
                    "candidates": {
                        "$push": {
                            "_id": "$_id",
                            "new_file": "$new_file",
                            "created_at": "$created_at"
                        }
                    }
                }
            },
            # 2. Sort by most recent
            {"$sort": {"created_at": -1}},  # Changed to sort by most recent first
            # 3. Facet for pagination
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "data": [{"$skip": skip}, {"$limit": page_size}]
                }
            }
        ]

        try:
            agg_result = await collection.aggregate(pipeline).to_list(None)
        except Exception as e:
            LOGGER.error(f"Search aggregation failed: {e}")
            return [], 0

        if not agg_result:
            return [], 0

        result_data = agg_result[0]["data"]
        total = agg_result[0]["metadata"][0]["total"] if agg_result[0]["metadata"] else 0

        enriched = []

        for item in result_data:
            # Reconstruct a flat-ish object but with candidates
            group_key = item["_id"]

            doc = {
                "_id": str(item["doc_id"]), # Representative ID
                "tmdb_id": group_key["tmdb_id"],
                "media_type": group_key["media_type"],
                "quality": group_key["quality"],
                "season": group_key.get("season"),
                "episode": group_key.get("episode"),
                "metadata": item["metadata"],
                "candidates": []
            }

            # Fetch active info for this group
            old_file = await self._get_active_file_info_internal(doc)

            doc["old_file"] = old_file

            # Skip auto-resolution checks for search results - we want to show all matches
            final_candidates = []
            for cand in item["candidates"]:
                cand_doc = convert_objectid_to_str(cand)
                final_candidates.append(cand_doc)

            doc["candidates"] = final_candidates
            enriched.append(doc)

        return enriched, total

