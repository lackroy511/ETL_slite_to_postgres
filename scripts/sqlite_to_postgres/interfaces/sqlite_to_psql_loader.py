import os

from scripts.sqlite_to_postgres.interfaces.postgres_connector import PostgreSQLMoviesDB
from scripts.sqlite_to_postgres.interfaces.sqlite_connector import SQLiteMoviesDB


class SQLiteToPSQLoader:
    SQLITE_DB_PATH = 'databases/movies.sqlite'
    PSQL_DSN = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('HOST'),
        'port': os.getenv('PORT'),
    }

    def load_movies(self):
        pass
