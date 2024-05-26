import os

from scripts.sqlite_to_postgres.interfaces.sqlite_to_psql_loader import SQLiteToPSQLoader

SQLITE_DB_PATH = 'databases/movies.sqlite'
PSQL_DSN = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT'),
}


def main():
    loader = SQLiteToPSQLoader(SQLITE_DB_PATH, PSQL_DSN)
    loader.init_psql_schema()
    loader.load_movies(load_step=500)
    loader.load_genres(load_step=500)


if __name__ == '__main__':
    main()
