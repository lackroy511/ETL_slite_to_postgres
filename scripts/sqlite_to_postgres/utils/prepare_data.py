import uuid
from datetime import datetime

import pytz

from scripts.sqlite_to_postgres.models.psql_models import (
    Film, FilmType, Genre, Person,
)


def prepare_psql_movies_data(movies: list[tuple]) -> list[Film]:
    new_movies = []
    
    for movie in movies:
        movie_data = {
            'id': str(uuid.uuid4()),
            'title': movie[5],
            'description': movie[6],
            'creation_date': None,
            'certificate': None,
            'file_path': None,
            'rating': float(movie[8]) if movie[8] else None,
            'type': FilmType.movie,
            'created_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
        }
        new_movies.append(
            tuple(dict(Film(**movie_data)).values()),
        )
    
    return new_movies

    
def prepare_psql_genres_data(genres: list[tuple]) -> list[Genre]:
    unique_genres = set()
    for genre in genres:
        if genre:
            genre = genre[1].split(',')
            genre = map(lambda genre: genre.replace(' ', ''), genre)
            unique_genres = set(genre).union(unique_genres)
    
    new_genres = []
    for genre in unique_genres:
        genre_data = {
            'id': str(uuid.uuid4()),
            'name': genre.replace(' ', ''),
            'description': None,
            'created_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
        }
        new_genres.append(
            tuple(dict(Genre(**genre_data)).values()),
        )
    
    return new_genres


def prepare_psql_person_data(persons_data: list[tuple]) -> list[Person]:
    persons = []
    for row in persons_data:
        for person in row[1:]:
            if person and person not in persons:
                persons.append(person)
    
    new_persons = []
    for person in persons:
        person_data = {
            'id': str(uuid.uuid4()),
            'full_name': person,
            'birth_date': None,
            'created_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
        }
        new_persons.append(
            tuple(dict(Person(**person_data)).values()),
        )
         
    return new_persons
