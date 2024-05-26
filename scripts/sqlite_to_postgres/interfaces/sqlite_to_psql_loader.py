from scripts.sqlite_to_postgres.interfaces.postgres_connector import \
    PostgreSQLMoviesDB
from scripts.sqlite_to_postgres.interfaces.sqlite_connector import \
    SQLiteMoviesDB
from scripts.sqlite_to_postgres.utils.prepare_data import (
    prepare_psql_genres_data, prepare_psql_movies_data, prepare_psql_person_data)

SQLITE_QUERIES_PATH = {
    'get_movies_count': 'sql/sqlite_movies/get_movies_count.sql',
    'get_movies_between': 'sql/sqlite_movies/get_movies_between.sql',
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


class SQLiteToPSQLoader:

    def __init__(self, sqlite_db_path: str, psql_dsn: dict) -> None:
        self.sqlite_db_path = sqlite_db_path
        self.psql_dsn = psql_dsn

    def init_psql_schema(self) -> None:
        with PostgreSQLMoviesDB(**self.psql_dsn) as psql_db:
            psql_db.init_schema(PSQL_QUERIES_PATH['schema_design'])

    def load_movies(self, load_step: int = 5000) -> None:
        with SQLiteMoviesDB(self.sqlite_db_path) as sqlite_db:
            movies_count = sqlite_db.get_entities_count(
                SQLITE_QUERIES_PATH['get_movies_count'],
            )

            from_row = 1
            to_row = load_step
            for _ in range(0, movies_count, load_step):
                movies = sqlite_db.get_entities_in_range(
                    SQLITE_QUERIES_PATH['get_movies_between'], from_row, to_row,
                )
                movies = prepare_psql_movies_data(movies)

                with PostgreSQLMoviesDB(**self.psql_dsn) as psql_db:
                    psql_db.insert_entities(
                        PSQL_QUERIES_PATH['insert_into_film_work'],  movies, load_step,
                    )

                from_row = to_row + 1
                to_row = from_row + load_step

    def load_genres(self, load_step: int = 5000) -> None:
        with SQLiteMoviesDB(self.sqlite_db_path) as sqlite_db:
            movies_count = sqlite_db.get_entities_count(
                SQLITE_QUERIES_PATH['get_movies_count'],
            )

            from_row = 1
            to_row = load_step
            for _ in range(0, movies_count, load_step):
                genres = sqlite_db.get_entities_in_range(
                    SQLITE_QUERIES_PATH['get_genres_between'], from_row, to_row,
                )
                genres = prepare_psql_genres_data(genres)

                with PostgreSQLMoviesDB(**self.psql_dsn) as psql_db:
                    psql_db.insert_entities(
                        PSQL_QUERIES_PATH['insert_into_genres'],  genres, load_step,
                    )

                from_row = to_row + 1
                to_row = from_row + load_step

    def load_persons(self, load_step: int = 5000) -> None:
        with SQLiteMoviesDB(self.sqlite_db_path) as sqlite_db:
            persons_count = sqlite_db.get_entities_count(
                SQLITE_QUERIES_PATH['get_persons_count'],
            )

            from_row = 1
            to_row = load_step
            for _ in range(0, persons_count, load_step):
                persons = sqlite_db.get_entities_in_range(
                    SQLITE_QUERIES_PATH['get_persons_between'], from_row, to_row,
                )
                persons = prepare_psql_person_data(persons)

                with PostgreSQLMoviesDB(**self.psql_dsn) as psql_db:
                    psql_db.insert_entities(
                        PSQL_QUERIES_PATH['insert_into_persons'],  persons, load_step,
                    )

                from_row = to_row + 1
                to_row = from_row + load_step
