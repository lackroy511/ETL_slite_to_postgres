import uuid
from datetime import datetime

import pytz
import json

from scripts.sqlite_to_postgres.cfg_variables import PSQL_DSN, SQLITE_DB_PATH
from scripts.sqlite_to_postgres.interfaces.postgres_connector import PostgreSQLConnector
from scripts.sqlite_to_postgres.interfaces.sqlite_connector import SQLiteConnector
from scripts.sqlite_to_postgres.models.psql_models import (
    Film, FilmType, FilmWorkGenre, FilmWorkPerson, Genre, Person,
)
from scripts.sqlite_to_postgres.utils.logic import (
    get_actors,
    get_directors,
    get_film_work_person_relations,
    get_list_of_persons_from_sql_data, 
    form_new_genres, 
    form_new_persons, 
    get_film_work_genre_relations, 
    get_unique_genres,
    get_writers,
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
    unique_genres = get_unique_genres(genres)
    new_genres = form_new_genres(unique_genres)
    
    return new_genres


def prepare_psql_person_data(persons_data: list[tuple]) -> list[Person]:
    persons = get_list_of_persons_from_sql_data(persons_data)
    new_persons = form_new_persons(persons)
         
    return new_persons


def prepare_psql_film_work_genre_data(sqlite_movies: list[tuple]) -> list[FilmWorkGenre]:
    film_work_genre_relations = get_film_work_genre_relations(sqlite_movies)
    
    return film_work_genre_relations


def prepare_psql_film_work_person_data(sqlite_movies: list[tuple]) -> list[FilmWorkPerson]: 
    film_work_person_relations = []
    for sqlite_movie in sqlite_movies:
        with SQLiteConnector(SQLITE_DB_PATH) as sqlite_db:
            sqlite_movie_id = sqlite_movie[1]
            persons_roles = {
                'director': [],
                'writer': [],
                'actor': [],
            }
            directors = get_directors(sqlite_movie)
            persons_roles['director'].extend(directors)
            
            writers = get_writers(sqlite_movie, sqlite_db)
            persons_roles['writer'].extend(writers)
            
            actors = get_actors(sqlite_movie_id, sqlite_db)
            persons_roles['actor'].extend(actors)
        
        with PostgreSQLConnector(**PSQL_DSN) as psql_db:
            film_work_person_relations.extend(
                get_film_work_person_relations(
                    sqlite_movie_id, psql_db, persons_roles,
                ),
            )
    
    return film_work_person_relations
