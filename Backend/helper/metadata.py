import asyncio
import traceback
import PTN
from re import compile, IGNORECASE
from Backend.helper.imdb import get_detail, get_season, search_title
from Backend.helper.pyro import extract_tmdb_id
from themoviedb import aioTMDb
from Backend.config import Telegram
import Backend
from Backend.logger import LOGGER
from Backend.helper.encrypt import encode_string

# ----------------- Configuration -----------------
DELAY = 0
tmdb = aioTMDb(key=Telegram.TMDB_API, language="en-US", region="US")

# Cache dictionaries (per run)
IMDB_CACHE: dict = {}
TMDB_SEARCH_CACHE: dict = {}
TMDB_DETAILS_CACHE: dict = {}
EPISODE_CACHE: dict = {}  

# Concurrency semaphore for external API calls
API_SEMAPHORE = asyncio.Semaphore(12)

# ----------------- Helpers -----------------
def format_tmdb_image(path: str, size="w500") -> str:
    if not path:
        return ""
    return f"https://image.tmdb.org/t/p/{size}{path}"

def format_imdb_images(imdb_id: str) -> dict:
    if not imdb_id:
        return {"poster": "", "backdrop": "", "logo": ""}
    return {
        "poster": f"https://images.metahub.space/poster/small/{imdb_id}/img",
        "backdrop": f"https://images.metahub.space/background/medium/{imdb_id}/img",
        "logo": f"https://images.metahub.space/logo/medium/{imdb_id}/img",
    }

async def safe_imdb_search(title: str, type_: str) -> str | None:
    """Safely search IMDb title and return its ID, with simple caching."""
    key = f"imdb::{type_}::{title}"
    if key in IMDB_CACHE:
        return IMDB_CACHE[key]
    try:
        async with API_SEMAPHORE:
            result = await search_title(query=title, type=type_)
        imdb_id = result["id"] if result else None
        IMDB_CACHE[key] = imdb_id
        return imdb_id
    except Exception as e:
        LOGGER.warning(f"IMDb search failed for '{title}' [{type_}]: {e}")
        return None

async def safe_tmdb_search(title: str, type_: str, year=None):
    """Safely search TMDb title with caching."""
    key = f"tmdb_search::{type_}::{title}::{year}"
    if key in TMDB_SEARCH_CACHE:
        return TMDB_SEARCH_CACHE[key]
    try:
        async with API_SEMAPHORE:
            if type_ == "movie":
                if year:
                    results = await tmdb.search().movies(query=title, year=year)
                else:
                    results = await tmdb.search().movies(query=title)
            else:
                results = await tmdb.search().tv(query=title)
        res = results[0] if results else None
        TMDB_SEARCH_CACHE[key] = res
        return res
    except Exception as e:
        LOGGER.error(f"TMDb search failed for '{title}' [{type_}]: {e}")
        TMDB_SEARCH_CACHE[key] = None
        return None

async def _tmdb_tv_details(tv_id):
    if tv_id in TMDB_DETAILS_CACHE:
        return TMDB_DETAILS_CACHE[tv_id]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.tv(tv_id).details(append_to_response="external_ids,credits")
        TMDB_DETAILS_CACHE[tv_id] = details
        return details
    except Exception as e:
        LOGGER.warning(f"TMDb tv details fetch failed for id={tv_id}: {e}")
        TMDB_DETAILS_CACHE[tv_id] = None
        return None

async def _tmdb_movie_details(movie_id):
    if movie_id in TMDB_DETAILS_CACHE:
        return TMDB_DETAILS_CACHE[movie_id]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.movie(movie_id).details(append_to_response="external_ids,credits")
        TMDB_DETAILS_CACHE[movie_id] = details
        return details
    except Exception as e:
        LOGGER.warning(f"TMDb movie details fetch failed for id={movie_id}: {e}")
        TMDB_DETAILS_CACHE[movie_id] = None
        return None

async def _tmdb_episode_details(tv_id, season, episode):
    key = (tv_id, season, episode)
    if key in EPISODE_CACHE:
        return EPISODE_CACHE[key]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.episode(tv_id, season, episode).details()
        EPISODE_CACHE[key] = details
        return details
    except Exception:
        EPISODE_CACHE[key] = None
        return None

# ----------------- Main Entry -----------------
async def metadata(filename: str, channel: int, msg_id) -> dict | None:
    try:
        parsed = PTN.parse(filename)
    except Exception as e:
        LOGGER.error(f"PTN parsing failed for {filename}: {e}\n{traceback.format_exc()}")
        return None

    # Skip combined/invalid files
    if "excess" in parsed and any("combined" in item.lower() for item in parsed["excess"]):
        LOGGER.info(f"Skipping {filename}: contains 'combined'")
        return None

    # Skip split/multipart files
    multipart_pattern = compile(r'(?:part|cd|disc|disk)[s._-]*\d+(?=\.\w+$)', IGNORECASE)
    if multipart_pattern.search(filename):
        LOGGER.info(f"Skipping {filename}: seems to be a split/multipart file")
        return None

    title = parsed.get("title")
    season = parsed.get("season")
    episode = parsed.get("episode")
    year = parsed.get("year")
    quality = parsed.get("resolution")

    if not quality:
        LOGGER.warning(f"Skipping {filename}: No resolution (parsed={parsed})")
        return None

    if isinstance(season, list) or isinstance(episode, list):
        LOGGER.warning(f"Invalid season/episode format for {filename}: {parsed}")
        return None

    if season and not episode:
        LOGGER.warning(f"Missing episode in {filename}: {parsed}")
        return None

    # Extract TMDb/IMDb hint
    default_id = None
    try:
        default_id = extract_tmdb_id(Backend.USE_DEFAULT_ID)
    except Exception:
        pass

    if not default_id:
        try:
            default_id = extract_tmdb_id(filename)
        except Exception:
            pass

    if not title:
        LOGGER.info(f"No title parsed from: {filename} (parsed={parsed})")
        return None

    data = {"chat_id": channel, "msg_id": msg_id}
    try:
        encoded_string = await encode_string(data)
    except Exception:
        encoded_string = None

    try:
        if season and episode:
            LOGGER.info(f"Fetching TV metadata: {title} S{season}E{episode}")
            return await fetch_tv_metadata(title, season, episode, encoded_string, year, quality, default_id)
        else:
            LOGGER.info(f"Fetching Movie metadata: {title} ({year})")
            return await fetch_movie_metadata(title, encoded_string, year, quality, default_id)
    except Exception as e:
        LOGGER.error(f"Error while fetching metadata for {filename}: {e}\n{traceback.format_exc()}")
        return None

# ----------------- TV Metadata -----------------
async def fetch_tv_metadata(title, season, episode, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = default_id if default_id and default_id.startswith("tt") else await safe_imdb_search(title, "tvSeries")
    tv_details, ep_details, use_tmdb = None, None, False

    # Try IMDb (cinemeta) first if imdb_id present
    if imdb_id:
        try:
            if imdb_id in IMDB_CACHE:
                tv_details = IMDB_CACHE[imdb_id]
            else:
                async with API_SEMAPHORE:
                    tv_details = await get_detail(imdb_id=imdb_id)
                IMDB_CACHE[imdb_id] = tv_details
            cache_key = f"{imdb_id}::{season}::{episode}"
            if cache_key in EPISODE_CACHE:
                ep_details = EPISODE_CACHE[cache_key]
            else:
                async with API_SEMAPHORE:
                    ep_details = await get_season(imdb_id=imdb_id, season_id=season, episode_id=episode)
                EPISODE_CACHE[cache_key] = ep_details
        except Exception as e:
            LOGGER.warning(f"IMDb TV fetch failed [{imdb_id}]: {e}")

    # If IMDb failed, fallback to TMDb
    if not tv_details:
        use_tmdb = True
        tmdb_result = await safe_tmdb_search(title, "tv")
        if not tmdb_result:
            LOGGER.warning(f"No TMDb result for '{title}'")
            return None

        tv_id = tmdb_result.id
        tv_details = await _tmdb_tv_details(tv_id)
        if not tv_details:
            LOGGER.warning(f"TMDb TV details fetch failed for '{title}' (id={tv_id})")
            return None

        ep_details = await _tmdb_episode_details(tv_id, season, episode)

    # TMDb-based return path
    if use_tmdb and tv_details:
        credits = getattr(tv_details, "credits", None) or {}
        cast_names = []
        if credits:
            cast_list = getattr(credits, "cast", []) or []
            for member in cast_list:
                name = getattr(member, "name", None) or getattr(member, "original_name", None)
                if name:
                    cast_names.append(name)

        return {
            "tmdb_id": tv_details.id,
            "imdb_id": getattr(tv_details, "external_ids", {}).imdb_id if getattr(tv_details, "external_ids", None) else getattr(tv_details, "imdb_id", None),
            "title": tv_details.name,
            "year": getattr(tv_details.first_air_date, "year", 0) if getattr(tv_details, "first_air_date", None) else 0,
            "rate": getattr(tv_details, "vote_average", 0) or 0,
            "description": tv_details.overview or "",
            "poster": format_tmdb_image(tv_details.poster_path),
            "backdrop": format_tmdb_image(tv_details.backdrop_path, "original"),
            "logo": "",
            "genres": [g.name for g in (tv_details.genres or [])],
            "media_type": "tv",
            "cast": cast_names,
            "season_number": season,
            "episode_number": episode,
            "episode_title": getattr(ep_details, "name", f"S{season}E{episode}") if ep_details else f"{tv_details.name} S{season}E{episode}",
            "episode_backdrop": format_tmdb_image(getattr(ep_details, "still_path", None), "original") if ep_details else "",
            "episode_overview": getattr(ep_details, "overview", "") if ep_details else "",
            "episode_released": (str(ep_details.air_date.strftime("%Y-%m-%dT05:00:00.000Z")) if getattr(ep_details, "air_date", None) else ""),
            "quality": quality,
            "encoded_string": encoded_string,
        }

    if not tv_details:
        LOGGER.warning(f"No valid IMDb data for {title}")
        return None

    imdb_id = tv_details.get("id", "")
    images = format_imdb_images(imdb_id)

    return {
        "tmdb_id": imdb_id.replace("tt", "") if imdb_id else "",
        "imdb_id": imdb_id,
        "title": tv_details.get("title", title),
        "year": tv_details.get("releaseDetailed", {}).get("year", 0),
        "rate": tv_details.get("rating", {}).get("star", 0),
        "description": tv_details.get("plot", ""),
        "poster": images["poster"],
        "backdrop": images["backdrop"],
        "logo": images["logo"],
        "cast": tv_details.get("cast", []),
        "genres": tv_details.get("genre", []),
        "media_type": "tv",
        "season_number": season,
        "episode_number": episode,
        "episode_title": ep_details.get("title", f"S{season}E{episode}") if ep_details else f"{tv_details.get('title', title)} S{season}E{episode}",
        "episode_backdrop": ep_details.get("image", "") if ep_details else "",
        "episode_overview": ep_details.get("plot", "") if ep_details else "",
        "episode_released": str(ep_details.get("released", "")) if ep_details else "",
        "quality": quality,
        "encoded_string": encoded_string,
    }

# ----------------- Movie Metadata -----------------
async def fetch_movie_metadata(title, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = default_id if default_id and default_id.startswith("tt") else await safe_imdb_search(f"{title} {year}" if year else title, "movie")
    movie_details, use_tmdb = None, False

    # Try IMDb first
    if imdb_id:
        try:
            if imdb_id in IMDB_CACHE:
                movie_details = IMDB_CACHE[imdb_id]
            else:
                async with API_SEMAPHORE:
                    movie_details = await get_detail(imdb_id=imdb_id)
                IMDB_CACHE[imdb_id] = movie_details
        except Exception as e:
            LOGGER.warning(f"IMDb movie fetch failed [{title}]: {e}")

    # Fallback to TMDb
    if not movie_details:
        use_tmdb = True
        tmdb_result = await safe_tmdb_search(title, "movie", year)
        if not tmdb_result:
            LOGGER.warning(f"No TMDb movie found for '{title}'")
            return None

        try:
            movie_details = await _tmdb_movie_details(tmdb_result.id)
        except Exception as e:
            LOGGER.warning(f"TMDb movie details failed for {title}: {e}")
            return None

    # TMDb result return
    if use_tmdb and movie_details:
        credits = getattr(movie_details, "credits", None) or {}
        cast_names = []
        if credits:
            cast_list = getattr(credits, "cast", []) or []
            for member in cast_list:
                name = getattr(member, "name", None) or getattr(member, "original_name", None)
                if name:
                    cast_names.append(name)
        return {
            "tmdb_id": movie_details.id,
            "imdb_id": movie_details.external_ids.imdb_id if getattr(movie_details, "external_ids", None) else None,
            "title": movie_details.title,
            "year": getattr(movie_details.release_date, "year", 0) if getattr(movie_details, "release_date", None) else 0,
            "rate": getattr(movie_details, "vote_average", 0) or 0,
            "description": movie_details.overview or "",
            "poster": format_tmdb_image(movie_details.poster_path),
            "backdrop": format_tmdb_image(movie_details.backdrop_path, "original"),
            "logo": "",
            "cast": cast_names,
            "media_type": "movie",
            "genres": [g.name for g in (movie_details.genres or [])],
            "quality": quality,
            "encoded_string": encoded_string,
        }

    # IMDb result return
    imdb_id = movie_details.get("id", "")
    images = format_imdb_images(imdb_id)

    return {
        "tmdb_id": imdb_id.replace("tt", "") if imdb_id else "",
        "imdb_id": imdb_id,
        "title": movie_details.get("title", title),
        "year": movie_details.get("releaseDetailed", {}).get("year", 0),
        "rate": movie_details.get("rating", {}).get("star", 0),
        "description": movie_details.get("plot", ""),
        "poster": images["poster"],
        "backdrop": images["backdrop"],
        "logo": images["logo"],
        "cast": movie_details.get("cast", []),
        "media_type": "movie",
        "genres": movie_details.get("genre", []),
        "quality": quality,
        "encoded_string": encoded_string,
    }
