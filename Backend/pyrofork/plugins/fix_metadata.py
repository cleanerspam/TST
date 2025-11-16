import asyncio
import time
from typing import Optional, Any, List

from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from Backend import db
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.metadata import fetch_tv_metadata, fetch_movie_metadata, API_SEMAPHORE
from Backend.logger import LOGGER

# Global cancel flag
CANCEL_REQUESTED = False

# Tunable parameters (safe defaults for TMDb free-tier)
MOVIE_BATCH = 500
TV_BATCH = 200
STATUS_UPDATE_INTERVAL = 50
CONCURRENCY_WORKERS = 4   # safe for TMDb free-tier (also used for concurrent episode fetches)
WORKER_MIN_DELAY = 0.0    # optional small delay to soften load

# Retry/backoff settings
MAX_FETCH_RETRIES = 4
BASE_BACKOFF_SECS = 0.6   # slightly larger base for free-tier safety


# -------------------------------
# Utility helpers
# -------------------------------
def progress_bar(done: int, total: int, length: int = 20) -> str:
    if total <= 0:
        return "[--------------------] 0/0"
    filled = int(length * (done / total))
    # clamp filled to [0, length]
    filled = max(0, min(length, filled))
    return f"[{'█' * filled}{'░' * (length - filled)}] {done}/{total}"


def format_eta(seconds: Optional[float]) -> str:
    if seconds is None or seconds == float("inf"):
        return "unknown"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {sec}s"
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


async def fetch_with_retries(fetch_coro_func, *args, **kwargs) -> Optional[Any]:
    """
    Generic retry wrapper with exponential backoff.
    Detects simple rate-limit signals (429 or 'rate limit' text) and increases backoff.
    Uses API_SEMAPHORE if provided by your project (optional global rate limiter).
    """
    backoff = BASE_BACKOFF_SECS
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            if API_SEMAPHORE is not None:
                # If your project provides an API_SEMAPHORE, use it to coordinate all API calls
                async with API_SEMAPHORE:
                    return await fetch_coro_func(*args, **kwargs)
            else:
                return await fetch_coro_func(*args, **kwargs)
        except Exception as e:
            err_text = str(e).lower()
            # detect rate-limit hints
            is_rate_limited = ("429" in err_text) or ("rate limit" in err_text) or ("too many requests" in err_text)
            if attempt == MAX_FETCH_RETRIES:
                LOGGER.error(f"Fetch failed after {attempt} attempts: {e}")
                return None
            # escalate backoff on rate-limit
            if is_rate_limited:
                extra = backoff * 2
                LOGGER.warning(f"Rate-limited on attempt {attempt}; backing off {extra:.2f}s (rate-limit). Error: {e}")
                await asyncio.sleep(extra)
                backoff *= 2
            else:
                LOGGER.warning(f"Fetch attempt {attempt} failed ({e}); backing off {backoff:.2f}s and retrying...")
                await asyncio.sleep(backoff)
                backoff *= 2
    return None


# -------------------------------
# Cancel callback handler
# -------------------------------
@Client.on_callback_query(filters.regex("cancel_fix"))
async def cancel_fix(_, query):
    global CANCEL_REQUESTED
    CANCEL_REQUESTED = True
    try:
        await query.message.edit_text("❌ Metadata fixing has been cancelled by the user.")
    except Exception:
        pass
    await query.answer("Cancelled")
    LOGGER.info("User requested metadata fix cancellation.")


# -------------------------------
# Main command handler
# -------------------------------
@Client.on_message(filters.command("fixmetadata") & filters.private & CustomFilters.owner, group=10)
async def fix_metadata_handler(_, message):
    """
    /fixmetadata          -> runs for items where metadata_update_done != True
    /fixmetadata force    -> run for everything (force refresh)
    """
    global CANCEL_REQUESTED
    CANCEL_REQUESTED = False

    cmd_args = message.text.split()
    force_mode = len(cmd_args) > 1 and cmd_args[1].lower() == "force"
    if force_mode:
        LOGGER.info("Starting /fixmetadata in FORCE mode (will refresh all items).")

    # Step 1: compute totals (movies + show-level + episodes to update)
    LOGGER.info("Counting items to process (initial scan).")
    total_movies = 0
    total_shows = 0
    total_episodes = 0

    storage_db_count = db.current_db_index

    for db_index in range(1, storage_db_count + 1):
        if CANCEL_REQUESTED:
            return
        key = f"storage_{db_index}"
        database = db.dbs[key]

        # Movies count
        if force_mode:
            movies_count = await database["movie"].count_documents({})
        else:
            movies_count = await database["movie"].count_documents({
                "$or": [{"metadata_update_done": {"$exists": False}}, {"metadata_update_done": False}]
            })
        total_movies += movies_count

        # TV shows (show-level)
        if force_mode:
            shows_count = await database["tv"].count_documents({})
        else:
            shows_count = await database["tv"].count_documents({
                "$or": [{"metadata_update_done": {"$exists": False}}, {"metadata_update_done": False}]
            })
        total_shows += shows_count

        # Episodes count
        skip = 0
        while True:
            docs = await database["tv"].find({}, {"seasons": 1}).skip(skip).limit(TV_BATCH).to_list(length=TV_BATCH)
            if not docs:
                break
            for tv in docs:
                seasons = tv.get("seasons") or []
                for s in seasons:
                    episodes = s.get("episodes") or []
                    if force_mode:
                        total_episodes += len(episodes)
                    else:
                        for ep in episodes:
                            if not ep.get("metadata_update_done", False):
                                total_episodes += 1
            skip += TV_BATCH

    TOTAL = total_movies + total_shows + total_episodes
    DONE = 0
    start_time = time.time()

    LOGGER.info(f"Metadata fixer will process: movies={total_movies}, shows={total_shows}, episodes={total_episodes}, total={TOTAL}")

    status = await message.reply_text(
        "⏳ Initializing metadata fixing...",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_fix")]])
    )

    async def maybe_update_status():
        nonlocal DONE
        elapsed = time.time() - start_time
        avg_time = elapsed / DONE if DONE else float("inf")
        eta = avg_time * (TOTAL - DONE) if TOTAL > DONE and avg_time != float("inf") else 0
        try:
            await status.edit_text(
                f"🔧 Fixing metadata...\n"
                f"{progress_bar(DONE, TOTAL)}\n"
                f"⏱ ETA: {format_eta(eta)}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_fix")]])
            )
        except Exception:
            # ignore UI edit errors
            pass

    # Semaphore bounds concurrency for movie & show-level workers and episodes
    # We will also use this semaphore inside concurrent episode fetches to limit parallel requests.
    semaphore = asyncio.Semaphore(CONCURRENCY_WORKERS)

    # ---------------------------
    # MOVIE UPDATE (added fields)
    # ---------------------------
    async def process_movie_document(collection, movie_doc):
        nonlocal DONE
        if CANCEL_REQUESTED:
            return

        tmdb_id = movie_doc.get("tmdb_id")
        title = movie_doc.get("title")
        year = movie_doc.get("release_year")

        async with semaphore:
            meta = await fetch_with_retries(
                fetch_movie_metadata,
                title=title,
                encoded_string=None,
                year=year,
                quality=None,
                default_id=None
            )
            if WORKER_MIN_DELAY:
                await asyncio.sleep(WORKER_MIN_DELAY)

        if meta:
            # Common movie fields + additional safe fields
            update_payload = {
                "imdb_id": meta.get("imdb_id"),
                "cast": meta.get("cast"),
                "description": meta.get("description"),
                "genres": meta.get("genres"),
                "poster": meta.get("poster"),
                "backdrop": meta.get("backdrop"),
                "logo": meta.get("logo"),                    # was missing previously
                "rating": meta.get("rate"),
                # Additional fields (safe: set if present, else None)
                "runtime": meta.get("runtime"),
                "tagline": meta.get("tagline"),
                "homepage": meta.get("homepage"),
                "production_companies": meta.get("production_companies"),
                "spoken_languages": meta.get("spoken_languages"),
                "keywords": meta.get("keywords"),
                "certification": meta.get("certification") or meta.get("release_certification"),
                "metadata_update_done": True
            }
            try:
                await collection.update_one({"tmdb_id": tmdb_id}, {"$set": update_payload})
                LOGGER.debug(f"Movie updated: {tmdb_id} - {title}")
            except Exception as e:
                LOGGER.error(f"Failed to update movie {tmdb_id}: {e}")
        else:
            LOGGER.warning(f"No metadata returned for movie {tmdb_id} ({title}); leaving metadata_update_done unset for retry later.")

        DONE += 1
        if DONE % STATUS_UPDATE_INTERVAL == 0:
            await maybe_update_status()

    # ---------------------------
    # SHOW-LEVEL UPDATE (added fields)
    # ---------------------------
    async def process_show_document(collection, tv_doc):
        nonlocal DONE
        if CANCEL_REQUESTED:
            return

        tmdb_id = tv_doc.get("tmdb_id")
        title = tv_doc.get("title")
        year = tv_doc.get("release_year")

        async with semaphore:
            meta = await fetch_with_retries(
                fetch_tv_metadata,
                title=title,
                season=1,
                episode=1,
                encoded_string=None,
                year=year,
                quality=None,
                default_id=None
            )
            if WORKER_MIN_DELAY:
                await asyncio.sleep(WORKER_MIN_DELAY)

        if meta:
            update_payload = {
                "imdb_id": meta.get("imdb_id"),
                "cast": meta.get("cast"),
                "description": meta.get("description"),
                "genres": meta.get("genres"),
                "poster": meta.get("poster"),
                "backdrop": meta.get("backdrop"),
                "logo": meta.get("logo"),                    # added logo
                "rating": meta.get("rate"),
                # Additional TV show fields
                "number_of_seasons": meta.get("number_of_seasons"),
                "number_of_episodes": meta.get("number_of_episodes"),
                "episode_run_time": meta.get("episode_run_time"),
                "networks": meta.get("networks"),
                "production_companies": meta.get("production_companies"),
                "homepage": meta.get("homepage"),
                "status": meta.get("status"),
                "tagline": meta.get("tagline"),
                "keywords": meta.get("keywords"),
                # Do NOT set "metadata_update_done" here — episodes logic will set it when fully done
            }
            try:
                await collection.update_one({"tmdb_id": tmdb_id}, {"$set": update_payload})
                LOGGER.debug(f"Show-level metadata updated for TMDB {tmdb_id}")
            except Exception as e:
                LOGGER.error(f"Failed to update show-level metadata for {tmdb_id}: {e}")
        else:
            LOGGER.warning(f"No show metadata for TMDB {tmdb_id} - {title}")

        DONE += 1
        if DONE % STATUS_UPDATE_INTERVAL == 0:
            await maybe_update_status()

    # ---------------------------
    # EPISODE FAILSAFE (concurrent episode fields)
    # ---------------------------
    async def failsafe_process_episodes(collection, original_tv_doc, force=False):
        """
        This version fetches episodes concurrently (bounded by the global `semaphore`).
        It:
          - only creates tasks for episodes that need updating (unless force=True),
          - uses array_filters to atomically update individual episodes (safe for concurrency),
          - aggregates per-season and per-show success and sets flags accordingly.
        """
        nonlocal DONE
        if CANCEL_REQUESTED:
            return

        tmdb_id = original_tv_doc.get("tmdb_id")
        title = original_tv_doc.get("title")
        year = original_tv_doc.get("release_year")

        # Re-fetch current doc state
        tv_latest = await collection.find_one({"tmdb_id": tmdb_id})
        if not tv_latest:
            LOGGER.error(f"[FAILSAFE] TV doc {tmdb_id} missing; skipping.")
            return

        seasons = tv_latest.get("seasons") or []
        overall_show_ok = True

        # Helper to process a single episode (runs under semaphore)
        async def _process_single_episode(s_num: int, e_num: int) -> bool:
            """
            Returns True if episode was updated successfully, False otherwise.
            """
            nonlocal DONE
            if CANCEL_REQUESTED:
                return False

            async with semaphore:
                try:
                    ep_meta = await fetch_with_retries(
                        fetch_tv_metadata,
                        title=title,
                        season=s_num,
                        episode=e_num,
                        encoded_string=None,
                        year=year,
                        quality=None,
                        default_id=None
                    )
                    if WORKER_MIN_DELAY:
                        await asyncio.sleep(WORKER_MIN_DELAY)
                except Exception as e:
                    LOGGER.error(f"[FAILSAFE] Exception during fetch for {tmdb_id} S{s_num}E{e_num}: {e}")
                    ep_meta = None

            if not ep_meta:
                LOGGER.warning(f"[FAILSAFE] Episode metadata missing for {tmdb_id} S{s_num}E{e_num}; will retry later.")
                # Count as attempted (so DONE increments), but return False so season/show not considered fully ok.
                DONE += 1
                if DONE % STATUS_UPDATE_INTERVAL == 0:
                    await maybe_update_status()
                return False

            # Prepare payload -- atomic update for the specific episode via array_filters
            ep_payload = {
                "seasons.$[s].episodes.$[e].overview": ep_meta.get("episode_overview") or ep_meta.get("overview"),
                "seasons.$[s].episodes.$[e].released": ep_meta.get("episode_released") or ep_meta.get("air_date"),
                "seasons.$[s].episodes.$[e].episode_backdrop": ep_meta.get("episode_backdrop") or ep_meta.get("still_path"),
                "seasons.$[s].episodes.$[e].episode_title": ep_meta.get("episode_title") or ep_meta.get("name"),
                "seasons.$[s].episodes.$[e].still_path": ep_meta.get("still_path"),
                "seasons.$[s].episodes.$[e].runtime": ep_meta.get("runtime") or ep_meta.get("episode_run_time"),
                "seasons.$[s].episodes.$[e].vote_average": ep_meta.get("vote_average"),
                "seasons.$[s].episodes.$[e].crew": ep_meta.get("crew"),
                "seasons.$[s].episodes.$[e].guest_stars": ep_meta.get("guest_stars"),
                "seasons.$[s].episodes.$[e].metadata_update_done": True
            }

            try:
                await collection.update_one(
                    {"tmdb_id": tmdb_id},
                    {"$set": ep_payload},
                    array_filters=[
                        {"s.season_number": s_num},
                        {"e.episode_number": e_num}
                    ]
                )
                LOGGER.debug(f"[FAILSAFE] Updated DB for {tmdb_id} S{s_num}E{e_num}")
                DONE += 1
                if DONE % STATUS_UPDATE_INTERVAL == 0:
                    await maybe_update_status()
                return True
            except Exception as e:
                LOGGER.error(f"[FAILSAFE] Failed DB update for {tmdb_id} S{s_num}E{e_num}: {e}")
                # Count as attempted
                DONE += 1
                if DONE % STATUS_UPDATE_INTERVAL == 0:
                    await maybe_update_status()
                return False

        # Iterate seasons; for each season create concurrent tasks for episodes in that season (bounded by semaphore)
        for season in seasons:
            if CANCEL_REQUESTED:
                return
            s_num = season.get("season_number")
            episodes = season.get("episodes") or []

            # Build tasks only for episodes that need updating (unless force=True)
            tasks: List[asyncio.Task] = []
            ep_number_to_task_index = {}  # helpful for mapping results to episodes
            for ep in episodes:
                if CANCEL_REQUESTED:
                    return
                e_num = ep.get("episode_number")
                already_done = ep.get("metadata_update_done", False)
                if (not force) and already_done:
                    # skip this episode
                    continue
                # schedule the episode processing
                task = asyncio.create_task(_process_single_episode(s_num, e_num))
                ep_number_to_task_index[e_num] = len(tasks)
                tasks.append(task)

            # If there are no tasks for this season, set season flag accordingly and continue
            if not tasks:
                # If all episodes in season were already done, mark season ok True
                try:
                    # set season-level metadata_update_done to True (if not set)
                    await collection.update_one(
                        {"tmdb_id": tmdb_id},
                        {"$set": {"seasons.$[s].metadata_update_done": True}},
                        array_filters=[{"s.season_number": s_num}]
                    )
                except Exception as e:
                    LOGGER.error(f"[FAILSAFE] Failed to set season flag for {tmdb_id} season {s_num}: {e}")
                    overall_show_ok = False
                continue

            # Run the tasks and gather results.
            # We limit concurrency through the semaphore used inside _process_single_episode.
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                LOGGER.error(f"[FAILSAFE] Exception while gathering episode tasks for {tmdb_id} season {s_num}: {e}")
                # If gather itself fails, consider season not fully ok.
                overall_show_ok = False
                # ensure pending tasks are cancelled
                for t in tasks:
                    if not t.done():
                        t.cancel()
                # continue to next season
                continue

            # Analyze results: results list contains True/False or exceptions
            season_ok = True
            for idx, res in enumerate(results):
                if isinstance(res, Exception):
                    LOGGER.error(f"[FAILSAFE] Task exception for {tmdb_id} season {s_num} - task idx {idx}: {res}")
                    season_ok = False
                    overall_show_ok = False
                elif res is not True:
                    # False or None means episode update failed
                    season_ok = False
                    overall_show_ok = False

            # After processing episodes of this season, set season-level flag
            try:
                await collection.update_one(
                    {"tmdb_id": tmdb_id},
                    {"$set": {"seasons.$[s].metadata_update_done": season_ok}},
                    array_filters=[{"s.season_number": s_num}]
                )
            except Exception as e:
                LOGGER.error(f"[FAILSAFE] Failed to set season flag for {tmdb_id} season {s_num}: {e}")
                overall_show_ok = False

        # Finalize show-level flag: only set if *all* seasons and episodes are done
        tv_final = await collection.find_one({"tmdb_id": tmdb_id})
        if not tv_final:
            LOGGER.error(f"[FAILSAFE] Could not re-fetch TV final state for {tmdb_id}")
            return

        all_done = True
        for season in (tv_final.get("seasons") or []):
            if not season.get("metadata_update_done", False):
                all_done = False
                break
            for ep in (season.get("episodes") or []):
                if not ep.get("metadata_update_done", False):
                    all_done = False
                    break
            if not all_done:
                break

        if all_done:
            try:
                await collection.update_one({"tmdb_id": tmdb_id}, {"$set": {"metadata_update_done": True}})
                LOGGER.info(f"[FAILSAFE] TV {tmdb_id} fully updated; show-level flag set.")
            except Exception as e:
                LOGGER.error(f"[FAILSAFE] Failed to set show-level flag for {tmdb_id}: {e}")
        else:
            LOGGER.info(f"[FAILSAFE] TV {tmdb_id} not fully updated; show-level flag left unset for retries.")

    # -------------------------
    # MOVIE PROCESSOR
    # -------------------------
    async def process_movies():
        for db_index in range(1, storage_db_count + 1):
            if CANCEL_REQUESTED:
                return
            collection = db.dbs[f"storage_{db_index}"]["movie"]
            skip = 0
            while True:
                if CANCEL_REQUESTED:
                    return
                if force_mode:
                    docs = await collection.find({}).skip(skip).limit(MOVIE_BATCH).to_list(length=MOVIE_BATCH)
                else:
                    docs = await collection.find(
                        {"$or": [{"metadata_update_done": {"$exists": False}}, {"metadata_update_done": False}]}
                    ).skip(skip).limit(MOVIE_BATCH).to_list(length=MOVIE_BATCH)

                if not docs:
                    break

                tasks = [asyncio.create_task(process_movie_document(collection, doc)) for doc in docs]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

                skip += MOVIE_BATCH

    # -------------------------
    # TV PROCESSOR
    # -------------------------
    async def process_tv():
        for db_index in range(1, storage_db_count + 1):
            if CANCEL_REQUESTED:
                return
            collection = db.dbs[f"storage_{db_index}"]["tv"]
            skip = 0
            while True:
                if CANCEL_REQUESTED:
                    return
                if force_mode:
                    tv_docs = await collection.find({}).skip(skip).limit(TV_BATCH).to_list(length=TV_BATCH)
                else:
                    tv_docs = await collection.find(
                        {"$or": [{"metadata_update_done": {"$exists": False}}, {"metadata_update_done": False}]}
                    ).skip(skip).limit(TV_BATCH).to_list(length=TV_BATCH)

                if not tv_docs:
                    break

                # Run show-level fetches concurrently (bounded)
                show_tasks = [asyncio.create_task(process_show_document(collection, tv_doc)) for tv_doc in tv_docs]
                if show_tasks:
                    await asyncio.gather(*show_tasks, return_exceptions=True)

                # Process episodes per-show using the concurrent failsafe (episodes inside a show are concurrent up to semaphore)
                for tv_doc in tv_docs:
                    if CANCEL_REQUESTED:
                        return
                    await failsafe_process_episodes(collection, tv_doc, force=force_mode)

                skip += TV_BATCH

    # Run processing
    try:
        LOGGER.info("Starting movie processing.")
        await process_movies()
        if CANCEL_REQUESTED:
            LOGGER.info("Cancelled after movie pass.")
            try:
                await status.edit_text("❌ Metadata fixing cancelled by user.")
            except Exception:
                pass
            return

        LOGGER.info("Starting TV processing.")
        await process_tv()

    except Exception as e:
        LOGGER.error(f"Unhandled error in metadata fixer: {e}")

    # Final notification
    if CANCEL_REQUESTED:
        try:
            await status.edit_text(
                f"❌ Metadata fixing cancelled.\n{progress_bar(DONE, TOTAL)}\n"
                f"⏱ Time elapsed: {format_eta(time.time() - start_time)}"
            )
        except Exception:
            pass
        LOGGER.info("Metadata fixing cancelled by user.")
        return

    try:
        await status.edit_text(
            f"🎉 Metadata Fix Completed!\n"
            f"{progress_bar(DONE, TOTAL)}\n"
            f"⏱ Time Taken: {format_eta(time.time() - start_time)}"
        )
    except Exception:
        pass

    LOGGER.info(f"Metadata fixing completed: processed {DONE}/{TOTAL} items.")
