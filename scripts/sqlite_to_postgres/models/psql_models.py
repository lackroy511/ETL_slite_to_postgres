from datetime import date, datetime
from enum import Enum
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel


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
    type: Literal[FilmType.movie, FilmType.series]
    sqlite_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime


class Genre(BaseModel):
    id: str
    name: str
    description: str | None = None
    created_at: datetime | None = None
    updated_at: datetime


class Person(BaseModel):
    id: str
    full_name: str
    birth_date: date | None = None
    created_at: datetime | None = None
    updated_at: datetime


class FilmWorkGenre(BaseModel):
    id: str
    film_work_id: str
    genre_id: str
    created_at: datetime


class FilmWorkPerson(BaseModel):
    id: str
    film_work_id: str
    person_id: str
    role: str
    created_at: datetime
