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


def prepare_psql_film_work_person_data(sqlite_movies: list[tuple]) -> list[FilmWorkPerson]: 
    film_work_person_relations = []
    for sqlite_movie in sqlite_movies:
        with SQLiteConnector(SQLITE_DB_PATH) as sqlite_db:
        
            persons_roles = {
                'director': [],
                'writer': [],
                'actor': [],
            }
            sqlite_movie_id = sqlite_movie[1]
            
            if sqlite_movie[3]:
                persons_roles['director'].extend(sqlite_movie[3].split(','))
            
            # ----------------------------------------------------------------
            
            if sqlite_movie[4]:
                sqlite_movie_writer_id = sqlite_movie[4]
                sqlite_db.cursor.execute(
                    f"SELECT name FROM writers WHERE id='{sqlite_movie_writer_id}'",
                )
                sqlite_movie_writer_name = sqlite_db.cursor.fetchone()[0]
                if sqlite_movie_writer_name:
                    persons_roles['writer'].append(sqlite_movie_writer_name)
            else:
                sqlite_movie_writers_ids = json.loads(sqlite_movie[9])
                
                for writer_id in sqlite_movie_writers_ids:
                    sqlite_db.cursor.execute(
                        f"SELECT name FROM writers WHERE id='{writer_id['id']}'",
                    )
                    sqlite_movie_writer_name = sqlite_db.cursor.fetchone()[0]
                    if sqlite_movie_writer_name:
                        persons_roles['writer'].append(sqlite_movie_writer_name)
            
            # ----------------------------------------------------------------
            
            sqlite_db.cursor.execute(
                f"SELECT name FROM actors JOIN movie_actors ON actors.id = movie_actors.actor_id WHERE movie_actors.movie_id = '{sqlite_movie_id}';",
            )
            actors = sqlite_db.cursor.fetchall()
            for actor in actors:
                if actor[0]:
                    persons_roles['actor'].append(actor[0])
        
        with PostgreSQLConnector(**PSQL_DSN) as psql_db:
            psql_db.cursor.execute(
                f"SELECT id FROM content.film_work WHERE sqlite_id='{sqlite_movie_id}'",
            )
            psql_movie_id = psql_db.cursor.fetchone()[0]
            
            for role, persons in persons_roles.items():
                psql_persons_ids = []
                for person in persons:
                    psql_db.cursor.execute(
                        'SELECT id FROM content.person WHERE full_name LIKE %s',
                        (f"%{person.lstrip(' ')}%",),
                    )
                    psql_person_id = psql_db.cursor.fetchone()[0]
                    if psql_person_id:
                        psql_persons_ids.append(psql_person_id)
                        
                    for psql_person_id in psql_persons_ids:
                        film_work_person_data = {
                            'id': str(uuid.uuid4()),
                            'film_work_id': psql_movie_id,
                            'person_id':  psql_person_id,
                            'role': role,
                            'created_at': datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
                        }
                        film_work_person_relations.append(
                            tuple(dict(FilmWorkPerson(**film_work_person_data)).values()),
                        )
    
    return film_work_person_relations
