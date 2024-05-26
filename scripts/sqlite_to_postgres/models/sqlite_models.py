from pydantic import BaseModel
from typing import Optional


class Movie(BaseModel):
    id: str
    genre: str
    director: str | None = None
    writer_id: str | None = None
    title: str
    plot: str | None = None
    ratings: str | int | None = None
    imdb_rating: float | None = None
    writers_ids: list[dict[str, str]] | None = None
