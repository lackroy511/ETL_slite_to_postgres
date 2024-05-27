import uuid
from datetime import datetime

import pytz

from scripts.sqlite_to_postgres.cfg_variables import PSQL_DSN, SQLITE_DB_PATH
from scripts.sqlite_to_postgres.interfaces.postgres_connector import PostgreSQLConnector
# from scripts.sqlite_to_postgres.main import PSQL_DSN, SQLITE_DB_PATH
from scripts.sqlite_to_postgres.models.psql_models import (
    Film, FilmType, FilmWorkGenre, Genre, Person,
)


def prepare_psql_movies_data(sqlite_movies: list[tuple]) -> list[Film]:
    new_movies = []
    
    for movie in sqlite_movies:
        movie_data = {
            'id': str(uuid.uuid4()),
            'title': movie[5],
            'description': movie[6],
            'creation_date': None,
            'certificate': None,
            'file_path': None,
            'rating': float(movie[8]) if movie[8] else None,
            'type': FilmType.movie,
            'sqlite_id': movie[1],
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
            if person:
                person = person.split(', ')
                for p in person:
                    if p not in persons:
                        persons.append(p)
    
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


def prepare_psql_film_work_genre_data(sqlite_movies: list[tuple]) -> list[FilmWorkGenre]:
    with PostgreSQLConnector(**PSQL_DSN) as cur:
        film_work_genre_relations = []
        
        for movie in sqlite_movies:
            genres = movie[2]
            sqlite_movie_id = movie[1]

            cur.cursor.execute(
                f"SELECT id FROM content.film_work WHERE sqlite_id='{sqlite_movie_id}'",
            )
            psql_movie_id = cur.cursor.fetchone()[0]
            
            for genre in genres.split(', '):
                cur.cursor.execute(
                    f"SELECT id FROM content.genre WHERE name='{genre}'",
                )
                psql_genre_id = cur.cursor.fetchone()[0]
                
                film_work_genre_data = {
                    'id': str(uuid.uuid4()),
                    'film_work_id': psql_movie_id,
                    'genre_id': psql_genre_id,
                    'created_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
                }
                film_work_genre_relations.append(
                    tuple(dict(FilmWorkGenre(**film_work_genre_data)).values()),
                )
    
    return film_work_genre_relations
