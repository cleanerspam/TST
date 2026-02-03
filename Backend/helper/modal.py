from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

# ---------------------------
# Quality Detail Schema
# ---------------------------
class QualityDetail(BaseModel):
    quality: str
    id: str
    name: str
    size: str
    dc_id: Optional[int] = None
    file_type: Optional[str] = "video"
    file_unique_id: Optional[str] = None
    tech_metadata: Optional[dict] = Field(default_factory=dict)


# ---------------------------
# Episode Schema
# ---------------------------
class Episode(BaseModel):
    episode_number: int
    title: str
    episode_backdrop: Optional[str] = None
    overview: Optional[str] = None
    released: Optional[str] = None
    telegram: Optional[List[QualityDetail]]


# ---------------------------
# Season Schema
# ---------------------------
class Season(BaseModel):
    season_number: int
    episodes: List[Episode] = Field(default_factory=list)


# ---------------------------
# TV Show Schema
# ---------------------------
class TVShowSchema(BaseModel):
    tmdb_id: Optional[int] = None
    imdb_id: Optional[str] = None
    db_index: int
    title: str
    genres: Optional[List[str]] = None
    description: Optional[str] = None
    rating: Optional[float] = None
    release_year: Optional[int] = None
    poster: Optional[str] = None
    backdrop: Optional[str] = None
    logo: Optional[str] = None
    cast: Optional[List[str]] = None
    runtime: Optional[str] = None
    media_type: str
    updated_on: datetime = Field(default_factory=datetime.utcnow)
    seasons: List[Season] = Field(default_factory=list)


# ---------------------------
# Movie Schema
# ---------------------------
class MovieSchema(BaseModel):
    tmdb_id: Optional[int] = None
    imdb_id: Optional[str] = None
    db_index: int
    title: str
    genres: Optional[List[str]] = None
    description: Optional[str] = None
    rating: Optional[float] = None
    release_year: Optional[int] = None
    poster: Optional[str] = None
    backdrop: Optional[str] = None
    logo: Optional[str] = None
    cast: Optional[List[str]] = None
    runtime: Optional[str] = None
    media_type: str
    updated_on: datetime = Field(default_factory=datetime.utcnow)
    telegram: Optional[List[QualityDetail]]


# ---------------------------
# Pending Update Schema
# ---------------------------
class PendingUpdateSchema(BaseModel):
    tmdb_id: Optional[int] = None
    media_type: str
    quality: str
    season: Optional[int] = None
    episode: Optional[int] = None
    new_file: dict
    metadata: dict
    created_at: datetime = Field(default_factory=datetime.utcnow)
