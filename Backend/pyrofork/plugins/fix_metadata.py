import asyncio
import time
import math
from typing import Optional, List, Dict, Any

from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from Backend import db
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.metadata import fetch_tv_metadata, fetch_movie_metadata
from Backend.logger import LOGGER

# Global cancel flag
CANCEL_REQUESTED = False

# Tunable parameters (safe defaults for TMDb free-tier)
MOVIE_BATCH = 500        # docs per page for movies
TV_BATCH = 200           # docs per page for tv shows
STATUS_UPDATE_INTERVAL = 50  # update Telegram status every N processed items
CONCURRENCY_WORKERS = 5  # concurrent movie/show-level workers (safe default)
EPISODE_SEQUENTIAL = True  # episodes processed sequentially per-show
WORKER_MIN_DELAY = 0.0   # optional per-worker delay (seconds) to soften API load

# Retry/backoff settings for TMDb fetches
MAX_FETCH_RETRIES = 3
BASE_BACKOFF_SECS = 0.5  # exponential backoff base


# -------------------------------
# Utility helpers
# -------------------------------
def progress_bar(done: int, total: int, length: int = 20) -> str:
    if total <= 0:
        return "[--------------------] 0/0"
    filled = int(length * (done / total))
    return f"[{'█' * filled}{'░' * (length - filled)}] {done}/{total}"


def format_eta(seconds: float) -> str:
    if seconds is None or seconds == float("inf"):
        return "unknown"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {sec}s"
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


async def fetch_with_retries(fetch_coro_func, *args, **kwargs):
    """
    Generic retry wrapper with exponential backoff for TMDb fetch coroutines.
    `fetch_coro_func` should be an async function (callable) that performs the fetch.
    Returns the result or None on permanent failure.
    """
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            result = await fetch_coro_func(*args, **kwargs)
            return result
        except Exception as e:
            # If it's the last attempt, log and return None
            if attempt == MAX_FETCH_RETRIES:
                LOGGER.error(f"Fetch failed after {attempt} attempts: {e}")
                return None
            backoff = BASE_BACKOFF_SECS * (2 ** (attempt - 1))
            LOGGER.warning(f"Fetch attempt {attempt} failed ({e}); backing off {backoff:.2f}s and retrying...")
            await asyncio.sleep(backoff)
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

    # Step 1: compute totals (movies + show-level + episodes to update) using pagination
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

        # Episodes count: iterate TV docs (fetch seasons only)
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
        eta = avg_time * (TOTAL - DONE) if TOTAL > DONE else 0
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

    # Semaphore bounds concurrency for movie & show-level workers
    semaphore = asyncio.Semaphore(CONCURRENCY_WORKERS)

    async def process_movie_document(collection, movie_doc):
        nonlocal DONE
        if CANCEL_REQUESTED:
            return

        tmdb_id = movie_doc.get("tmdb_id")
        title = movie_doc.get("title")
        year = movie_doc.get("release_year")

        # Acquire semaphore
        async with semaphore:
            # fetch with retries
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
            try:
                await collection.update_one(
                    {"tmdb_id": tmdb_id},
                    {"$set": {
                        "imdb_id": meta.get("imdb_id"),
                        "cast": meta.get("cast"),
                        "description": meta.get("description"),
                        "genres": meta.get("genres"),
                        "poster": meta.get("poster"),
                        "backdrop": meta.get("backdrop"),
                        "logo": meta.get("logo"),
                        "rating": meta.get("rate"),
                        "metadata_update_done": True
                    }}
                )
                LOGGER.debug(f"Movie updated: {tmdb_id} - {title}")
            except Exception as e:
                LOGGER.error(f"Failed to update movie {tmdb_id}: {e}")
        else:
            LOGGER.warning(f"No metadata returned for movie {tmdb_id} ({title}); leaving metadata_update_done unset for retry later.")

        DONE += 1
        if DONE % STATUS_UPDATE_INTERVAL == 0:
            await maybe_update_status()

    async def process_show_document(collection, tv_doc):
        """
        Fetch show-level metadata concurrently (bounded) and update fields (but NOT show-level 'done' flag).
        Episode processing will set episode/season/show flags.
        """
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
            try:
                await collection.update_one(
                    {"tmdb_id": tmdb_id},
                    {"$set": {
                        "imdb_id": meta.get("imdb_id"),
                        "cast": meta.get("cast"),
                        "description": meta.get("description"),
                        "genres": meta.get("genres"),
                        "poster": meta.get("poster"),
                        "backdrop": meta.get("backdrop"),
                        "logo": meta.get("logo"),
                        "rating": meta.get("rate"),
                        # Do NOT set show-level metadata_update_done here.
                    }}
                )
                LOGGER.debug(f"Show-level metadata updated for TMDB {tmdb_id}")
            except Exception as e:
                LOGGER.error(f"Failed to update show-level metadata for {tmdb_id}: {e}")
        else:
            LOGGER.warning(f"No show metadata for TMDB {tmdb_id} - {title}")

        DONE += 1
        if DONE % STATUS_UPDATE_INTERVAL == 0:
            await maybe_update_status()

    async def failsafe_process_episodes(collection, original_tv_doc, force=False):
        """
        FAIL-SAFE episode processor:
        - Re-fetch freshest doc
        - For each season -> episode:
            - If episode.metadata_update_done is True (and not force) -> skip
            - Fetch ep metadata with retries
            - If metadata returned, perform atomic update with arrayFilters setting ep fields + metadata_update_done=True
            - If any episode in a season fails, season.metadata_update_done remains False
        - After processing all episodes, re-fetch doc, set per-season flags where applicable,
          and set show-level metadata_update_done only if all episodes in all seasons are done.
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

        for season in seasons:
            if CANCEL_REQUESTED:
                return
            s_num = season.get("season_number")
            episodes = season.get("episodes") or []
            season_ok = True

            for ep in episodes:
                if CANCEL_REQUESTED:
                    return
                e_num = ep.get("episode_number")

                # If already updated and not force, skip
                if (not force) and ep.get("metadata_update_done", False):
                    continue

                # Fetch metadata with retries
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

                if not ep_meta:
                    LOGGER.warning(f"[FAILSAFE] Episode metadata missing for {tmdb_id} S{s_num}E{e_num}; will retry later.")
                    season_ok = False
                    overall_show_ok = False
                    # Don't update flags for this episode; leave metadata_update_done unset for retry
                else:
                    # Atomic update of episode fields + metadata_update_done = True
                    try:
                        await collection.update_one(
                            {"tmdb_id": tmdb_id},
                            {"$set": {
                                "seasons.$[s].episodes.$[e].overview": ep_meta.get("episode_overview"),
                                "seasons.$[s].episodes.$[e].released": ep_meta.get("episode_released"),
                                "seasons.$[s].episodes.$[e].episode_backdrop": ep_meta.get("episode_backdrop"),
                                "seasons.$[s].episodes.$[e].metadata_update_done": True
                            }},
                            array_filters=[
                                {"s.season_number": s_num},
                                {"e.episode_number": e_num}
                            ]
                        )
                    except Exception as e:
                        LOGGER.error(f"[FAILSAFE] Failed DB update for {tmdb_id} S{s_num}E{e_num}: {e}")
                        season_ok = False
                        overall_show_ok = False

                DONE += 1
                if DONE % STATUS_UPDATE_INTERVAL == 0:
                    await maybe_update_status()

            # After processing episodes of this season, set season-level flag if season_ok is True
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
    # Main loops: movies then tv
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

                tasks = []
                for doc in docs:
                    if CANCEL_REQUESTED:
                        return
                    tasks.append(asyncio.create_task(process_movie_document(collection, doc)))

                if tasks:
                    # gather; workers internally bounded by semaphore
                    await asyncio.gather(*tasks, return_exceptions=True)

                skip += MOVIE_BATCH

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
                show_tasks = []
                for tv_doc in tv_docs:
                    if CANCEL_REQUESTED:
                        return
                    show_tasks.append(asyncio.create_task(process_show_document(collection, tv_doc)))

                if show_tasks:
                    await asyncio.gather(*show_tasks, return_exceptions=True)

                # Process episodes sequentially per-show (failsafe)
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
