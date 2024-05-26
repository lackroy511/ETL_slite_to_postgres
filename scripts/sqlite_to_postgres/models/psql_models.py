from pydantic import BaseModel
from typing import Literal, Optional
from uuid import UUID
from datetime import date, datetime
from enum import Enum


class FilmType(str, Enum):
    movie = 'movie'
    series = 'series'


class Film(BaseModel):
    id: str
    title: str
    description: str | None = None
    creation_date: date | None = None
    certificate: str | None = None
    file_path: str | None = None
    rating: float | None = None
    type: Literal['movie', 'series']
    created_at: datetime | None = None
    updated_at: datetime | None = None
