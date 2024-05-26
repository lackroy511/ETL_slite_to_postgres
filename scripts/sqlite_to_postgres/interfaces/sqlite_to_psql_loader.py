import os

from scripts.sqlite_to_postgres.interfaces.postgres_connector import PostgreSQLMoviesDB
from scripts.sqlite_to_postgres.interfaces.sqlite_connector import SQLiteMoviesDB
from scripts.sqlite_to_postgres.utils.film_work import prepare_psql_movies_data


class SQLiteToPSQLoader:
    
    def __init__(self, sqlite_db_path: str, psql_dsn: dict) -> None:
        self.sqlite_db_path = sqlite_db_path
        self.psql_dsn = psql_dsn
        
    def init_psql_schema(self) -> None:
        with PostgreSQLMoviesDB(**self.psql_dsn) as psql_db:
            psql_db.init_schema()

    def load_movies(self, load_step: int = 5000) -> None:
        with SQLiteMoviesDB(self.sqlite_db_path) as sqlite_db:
            movies_count = sqlite_db.get_movies_count()
            movies_count = movies_count[0][0]

            for i in range(1, movies_count, load_step):
                movies = sqlite_db.get_movies(i, i + load_step)
                movies = prepare_psql_movies_data(movies)

                with PostgreSQLMoviesDB(**self.psql_dsn) as psql_db:
                    psql_db.insert_to_film_work(movies, load_step)
