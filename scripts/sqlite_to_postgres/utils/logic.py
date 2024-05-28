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


def get_unique_genres(genres: list[tuple]) -> set[str]:
    unique_genres = set()
    for genre in genres:
        if genre:
            genre = genre[1].split(',')
            genre = map(lambda genre: genre.replace(' ', ''), genre)
            unique_genres = set(genre).union(unique_genres)
    
    return unique_genres


def form_new_genres(unique_genres: set[str]) -> list[Genre]:
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


def get_list_of_persons_from_sql_data(persons_data: list[tuple]) -> list[str]:
    persons = []
    for row in persons_data:
        for person in row[1:]:
            if person:
                person = person.split(', ')
                for p in person:
                    if p not in persons:
                        persons.append(p)
    
    return persons


def form_new_persons(persons: list[str]) -> list[Person]:
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


def get_film_work_genre_relations(sqlite_movies: list[tuple]) -> list[FilmWorkGenre]:
    with PostgreSQLConnector(**PSQL_DSN) as psql_db:
        film_work_genre_relations = []
        for movie in sqlite_movies:
            sqlite_movie_id = movie[1]
            psql_db.cursor.execute(
                f"SELECT id FROM content.film_work WHERE sqlite_id='{sqlite_movie_id}'",
            )
            psql_movie_id = psql_db.cursor.fetchone()[0]
            genres = movie[2]
            
            relations = form_film_work_genre_relations(genres, psql_db, psql_movie_id)
            film_work_genre_relations.extend(relations)
    
    return film_work_genre_relations


def form_film_work_genre_relations(
        genres: str, 
        psql_db: PostgreSQLConnector,
        psql_movie_id: str) -> list[FilmWorkGenre]:
    
    relations = []
    for genre in genres.split(', '):
        psql_db.cursor.execute(
            f"SELECT id FROM content.genre WHERE name='{genre}'",
        )
        psql_genre_id = psql_db.cursor.fetchone()[0]
        
        film_work_genre_data = {
            'id': str(uuid.uuid4()),
            'film_work_id': psql_movie_id,
            'genre_id': psql_genre_id,
            'created_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
        }
        relations.append(
            tuple(dict(FilmWorkGenre(**film_work_genre_data)).values()),
        )
    
    return relations


def get_directors(sqlite_movie: tuple) -> dict[str, list[str]]:
    directors = []
    if sqlite_movie[3]:
        directors = sqlite_movie[3].split(',')
    
    return directors


def get_writers(
        sqlite_movie: tuple,
        sqlite_db: SQLiteConnector) -> list[str]:
    
    writers = []
    
    if sqlite_movie[4]:
        sqlite_movie_writer_id = sqlite_movie[4]
        sqlite_db.cursor.execute(
            f"SELECT name FROM writers WHERE id='{sqlite_movie_writer_id}'",
        )
        sqlite_movie_writer_name = sqlite_db.cursor.fetchone()[0]
        if sqlite_movie_writer_name:
            writers.append(sqlite_movie_writer_name)
    else:
        sqlite_movie_writers_ids = json.loads(sqlite_movie[9])
        
        for writer_id in sqlite_movie_writers_ids:
            sqlite_db.cursor.execute(
                f"SELECT name FROM writers WHERE id='{writer_id['id']}'",
            )
            sqlite_movie_writer_name = sqlite_db.cursor.fetchone()[0]
            if sqlite_movie_writer_name:
                writers.append(sqlite_movie_writer_name)

    return writers


def get_actors(
        sqlite_movie_id: tuple,
        sqlite_db: SQLiteConnector) -> list[str]:
    
    list_of_actors = []
    
    sqlite_db.cursor.execute(
        'SELECT name FROM actors JOIN movie_actors ON actors.id = movie_actors.actor_id' +
        f" WHERE movie_actors.movie_id = '{sqlite_movie_id}';",
    )
    actors = sqlite_db.cursor.fetchall()
    for actor in actors:
        for name in actor:
            if name:
                list_of_actors.append(name)
                
    return list_of_actors


def get_film_work_person_relations(
        sqlite_movie_id: tuple,
        psql_db: PostgreSQLConnector,
        persons_roles: dict[str, list[str]]) -> list[FilmWorkPerson]:
    
    psql_db.cursor.execute(
        f"SELECT id FROM content.film_work WHERE sqlite_id='{sqlite_movie_id}'",
    )
    psql_movie_id = psql_db.cursor.fetchone()[0]
    
    film_work_person_relations = []
    for role, persons in persons_roles.items():
        psql_persons_ids = []
        for person_name in persons:
            person_id = get_psql_person_id(person_name, psql_db)
            psql_persons_ids.extend(person_id)
                
            film_person_relations = get_film_person_relations(
                psql_persons_ids, psql_movie_id, role,
            )
            film_work_person_relations.extend(film_person_relations)
            
    return film_work_person_relations


def get_psql_person_id(
        person_name: str,
        psql_db: PostgreSQLConnector) -> list[str]:
    
    psql_db.cursor.execute(
        'SELECT id FROM content.person WHERE full_name LIKE %s',
        (f"%{person_name.lstrip(' ')}%",),
    )
    psql_person_id = psql_db.cursor.fetchone()[0]
    if psql_person_id:
        return [psql_person_id]

    return []


def get_film_person_relations(
        psql_persons_ids: list[str],
        psql_movie_id: str,
        role: str) -> list[FilmWorkPerson]:
    relations = []
    for psql_person_id in psql_persons_ids:
        film_work_person_data = {
            'id': str(uuid.uuid4()),
            'film_work_id': psql_movie_id,
            'person_id':  psql_person_id,
            'role': role,
            'created_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
        }
        relations.append(
            tuple(dict(FilmWorkPerson(**film_work_person_data)).values()),
        )
    
    return relations
