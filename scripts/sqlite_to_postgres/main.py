import os

from scripts.sqlite_to_postgres.interfaces.sqlite_to_psql_loader import \
    SQLiteToPSQLoader

from scripts.sqlite_to_postgres.utils.prepare_data import (
    prepare_psql_genres_data, prepare_psql_movies_data, prepare_psql_person_data,
)

SQLITE_QUERIES_PATH = {
    'get_movies_count': 'sql/sqlite_movies/get_movies_count.sql',
    'get_movies_between': 'sql/sqlite_movies/get_movies_between.sql',
    'get_genres_count': 'sql/sqlite_movies/get_genres_count.sql',
    'get_genres_between': 'sql/sqlite_movies/get_genres_between.sql',
    'get_persons_count': 'sql/sqlite_movies/get_persons_count.sql',
    'get_persons_between': 'sql/sqlite_movies/get_persons_between.sql',
}
PSQL_QUERIES_PATH = {
    'schema_design': 'sql/schema_design.sql',
    'insert_into_film_work': 'sql/postgres_movies/insert_into_film_work.sql',
    'insert_into_genres': 'sql/postgres_movies/insert_into_genres.sql',
    'insert_into_persons': 'sql/postgres_movies/insert_into_persons.sql',
}
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
    # Загрузить фильмы
    loader.load_entities(
        SQLITE_QUERIES_PATH['get_movies_count'],
        SQLITE_QUERIES_PATH['get_movies_between'],
        PSQL_QUERIES_PATH['insert_into_film_work'],
        prepare_psql_movies_data,
    )
    # Загрузить жанры
    loader.load_entities(
        SQLITE_QUERIES_PATH['get_genres_count'],
        SQLITE_QUERIES_PATH['get_genres_between'],
        PSQL_QUERIES_PATH['insert_into_genres'],
        prepare_psql_genres_data,
    )
    # Загрузить людей
    loader.load_entities(
        SQLITE_QUERIES_PATH['get_persons_count'],
        SQLITE_QUERIES_PATH['get_persons_between'],
        PSQL_QUERIES_PATH['insert_into_persons'],
        prepare_psql_person_data,
    )


if __name__ == '__main__':
    main()
