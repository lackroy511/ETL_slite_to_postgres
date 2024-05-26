from pydantic import BaseModel
from typing import Literal
from uuid import UUID
from datetime import date, datetime
from enum import Enum


class FilmType(str, Enum):
    movie = 'movie'
    series = 'series'


class Film(BaseModel):
    id: UUID
    title: str
    description: str | None = None
    creation_date: date | None = None
    certificate: str | None = None
    file_path: str | None = None
    rating: float | None = None
    type: Literal[FilmType.movie, FilmType.series]
    created_at: datetime
    updated_at: datetime
