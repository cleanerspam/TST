import time
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from Backend import db
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.metadata import fetch_tv_metadata, fetch_movie_metadata

# Global cancel flag (controlled by callback button)
CANCEL_REQUESTED = False

# Tunable batch sizes
MOVIE_BATCH = 500       # number of movie docs fetched per DB batch
TV_BATCH = 200          # number of tv docs fetched per DB batch
STATUS_UPDATE_INTERVAL = 50  # update status every N processed items


# -------------------------------
# Helpers
# -------------------------------
def progress_bar(done, total, length=20):
    if total <= 0:
        return "[--------------------] 0/0"
    filled = int(length * (done / total))
    return f"[{'█' * filled}{'░' * (length - filled)}] {done}/{total}"


def format_eta(seconds):
    if seconds is None or seconds == float("inf"):
        return "unknown"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {sec}s"
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


# -------------------------------
# Cancel button handler
# -------------------------------
@Client.on_callback_query(filters.regex("cancel_fix"))
async def cancel_fix(_, query):
    global CANCEL_REQUESTED
    CANCEL_REQUESTED = True
    # Edit message to show cancellation
    try:
        await query.message.edit_text("❌ Metadata fixing has been cancelled by the user.")
    except Exception:
        pass
    await query.answer("Cancelled")


# -------------------------------
# Main command
# -------------------------------
@Client.on_message(filters.command("fixmetadata") & filters.private & CustomFilters.owner, group=10)
async def fix_metadata_handler(_, message):
    global CANCEL_REQUESTED
    CANCEL_REQUESTED = False

    # Count totals correctly: movies + tv shows (show-level) + all episodes (episode-level)
    total_movies = 0
    total_tv_shows = 0
    total_episodes = 0

    # Gather an estimate of totals by scanning tv documents to count episodes.
    # This is only an initial pass to compute progress; it's acceptable overhead for ~5k shows.
    for i in range(1, db.current_db_index + 1):
        key = f"storage_{i}"
        database = db.dbs[key]

        # movie and tv counts
        total_movies += await database["movie"].count_documents({})
        total_tv_shows += await database["tv"].count_documents({})

        # Count episodes by iterating tv docs and summing episodes lengths.
        # We fetch only 'seasons' field to reduce network payload.
        # If there are many TV docs this is still reasonable for 5k shows.
        skip = 0
        page = 0
        while True:
            docs = await database["tv"].find({}, {"seasons": 1}).skip(skip).limit(TV_BATCH).to_list(length=TV_BATCH)
            if not docs:
                break
            for tv in docs:
                seasons = tv.get("seasons") or []
                for s in seasons:
                    eps = s.get("episodes") or []
                    total_episodes += len(eps)
            page += 1
            skip = page * TV_BATCH

    # TOTAL counts show-level updates + episode-level updates + movies
    TOTAL = total_movies + total_tv_shows + total_episodes
    DONE = 0
    start_time = time.time()

    status = await message.reply_text(
        "⏳ Initializing metadata fixing...",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_fix")]])
    )

    # -------------------------
    # Helper to update status periodically
    # -------------------------
    async def maybe_update_status():
        nonlocal DONE
        if DONE == 0:
            return
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
            # ignore edit exceptions (rate limits, deleted message, etc.)
            pass

    # -------------------------
    # Update movies using pagination (no noCursorTimeout)
    # -------------------------
    async def update_movies():
        nonlocal DONE
        for i in range(1, db.current_db_index + 1):
            if CANCEL_REQUESTED:
                return
            key = f"storage_{i}"
            collection = db.dbs[key]["movie"]

            skip = 0
            while True:
                if CANCEL_REQUESTED:
                    return

                docs = await collection.find({}).skip(skip).limit(MOVIE_BATCH).to_list(length=MOVIE_BATCH)
                if not docs:
                    break

                for movie in docs:
                    if CANCEL_REQUESTED:
                        return

                    tmdb_id = movie.get("tmdb_id")
                    title = movie.get("title")
                    year = movie.get("release_year")

                    try:
                        meta = await fetch_movie_metadata(
                            title=title,
                            encoded_string=None,
                            year=year,
                            quality=None,
                            default_id=None
                        )

                        if meta:
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
                                }}
                            )
                    except Exception:
                        # We don't want a single failure to abort the whole run.
                        # Loggers are in your project — keep silent here to avoid dependency.
                        pass

                    DONE += 1
                    if DONE % STATUS_UPDATE_INTERVAL == 0:
                        await maybe_update_status()

                skip += MOVIE_BATCH

    # -------------------------
    # Update TV shows (show-level + episode-level) using pagination
    # -------------------------
    async def update_tv():
        nonlocal DONE
        for i in range(1, db.current_db_index + 1):
            if CANCEL_REQUESTED:
                return
            key = f"storage_{i}"
            collection = db.dbs[key]["tv"]

            skip = 0
            while True:
                if CANCEL_REQUESTED:
                    return

                tv_docs = await collection.find({}).skip(skip).limit(TV_BATCH).to_list(length=TV_BATCH)
                if not tv_docs:
                    break

                for tv in tv_docs:
                    if CANCEL_REQUESTED:
                        return

                    tmdb_id = tv.get("tmdb_id")
                    title = tv.get("title")
                    year = tv.get("release_year")

                    # Show-level metadata update
                    try:
                        meta = await fetch_tv_metadata(
                            title=title,
                            season=1,
                            episode=1,
                            encoded_string=None,
                            year=year,
                            quality=None,
                            default_id=None
                        )

                        if meta:
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
                                }}
                            )
                    except Exception:
                        pass

                    DONE += 1  # count the show-level update
                    if DONE % STATUS_UPDATE_INTERVAL == 0:
                        await maybe_update_status()

                    # Episode-level updates: iterate seasons & episodes stored in the document
                    seasons = tv.get("seasons") or []
                    for season in seasons:
                        if CANCEL_REQUESTED:
                            return
                        s_num = season.get("season_number")
                        eps = season.get("episodes") or []
                        for ep in eps:
                            if CANCEL_REQUESTED:
                                return
                            e_num = ep.get("episode_number")

                            try:
                                ep_meta = await fetch_tv_metadata(
                                    title=title,
                                    season=s_num,
                                    episode=e_num,
                                    encoded_string=None,
                                    year=year,
                                    quality=None,
                                    default_id=None
                                )

                                if ep_meta:
                                    # Use arrayFilters to update the specific episode fields
                                    await collection.update_one(
                                        {"tmdb_id": tmdb_id},
                                        {"$set": {
                                            "seasons.$[s].episodes.$[e].overview": ep_meta.get("episode_overview"),
                                            "seasons.$[s].episodes.$[e].released": ep_meta.get("episode_released"),
                                            "seasons.$[s].episodes.$[e].episode_backdrop": ep_meta.get("episode_backdrop"),
                                        }},
                                        array_filters=[
                                            {"s.season_number": s_num},
                                            {"e.episode_number": e_num}
                                        ]
                                    )
                            except Exception:
                                # ignore single-episode failures
                                pass

                            DONE += 1  # count each episode update
                            if DONE % STATUS_UPDATE_INTERVAL == 0:
                                await maybe_update_status()

                skip += TV_BATCH

    # Run both steps
    await update_movies()
    if not CANCEL_REQUESTED:
        await update_tv()

    if CANCEL_REQUESTED:
        # aborted by user; final status edit
        try:
            await status.edit_text(
                f"❌ Metadata fixing cancelled.\n{progress_bar(DONE, TOTAL)}\n"
                f"⏱ Time elapsed: {format_eta(time.time() - start_time)}"
            )
        except Exception:
            pass
        return

    # Completed
    try:
        await status.edit_text(
            f"🎉 Metadata Fix Completed!\n"
            f"{progress_bar(DONE, TOTAL)}\n"
            f"⏱ Time Taken: {format_eta(time.time() - start_time)}"
        )
    except Exception:
        pass
