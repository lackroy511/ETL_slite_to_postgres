import os

from scripts.sqlite_to_postgres.cfg_variables import PSQL_DSN, PSQL_QUERIES_PATH, SQLITE_DB_PATH, SQLITE_QUERIES_PATH
from scripts.sqlite_to_postgres.interfaces.sqlite_to_psql_loader import \
    SQLiteToPSQLoader

from scripts.sqlite_to_postgres.utils.prepare_data import (
    prepare_psql_film_work_genre_data, prepare_psql_film_work_person_data, prepare_psql_genres_data, prepare_psql_movies_data, prepare_psql_person_data,
)


def main():
    loader = SQLiteToPSQLoader(SQLITE_DB_PATH, PSQL_DSN)
    loader.init_psql_schema()
    # Загрузить фильмы
    loader.load_entities(
        SQLITE_QUERIES_PATH['get_movies_count'],
        SQLITE_QUERIES_PATH['get_movies_between'],
        PSQL_QUERIES_PATH['insert_into_film_work'],
        prepare_psql_movies_data,
        load_step=500,
    )
    # Загрузить жанры
    loader.load_entities(
        SQLITE_QUERIES_PATH['get_genres_count'],
        SQLITE_QUERIES_PATH['get_genres_between'],
        PSQL_QUERIES_PATH['insert_into_genres'],
        prepare_psql_genres_data,
        load_step=500,
    )
    # Загрузить людей
    loader.load_entities(
        SQLITE_QUERIES_PATH['get_persons_count'],
        SQLITE_QUERIES_PATH['get_persons_between'],
        PSQL_QUERIES_PATH['insert_into_persons'],
        prepare_psql_person_data,
        load_step=500,
    )
    # Заполнить m2m связь фильм-жанр
    loader.load_entities(
        SQLITE_QUERIES_PATH['get_movies_count'],
        SQLITE_QUERIES_PATH['get_movies_between'],
        PSQL_QUERIES_PATH['insert_into_film_work_genre'],
        prepare_psql_film_work_genre_data,
        load_step=500,
    )
    # Заполнить m2m связь фильм-человек
    loader.load_entities(
        SQLITE_QUERIES_PATH['get_persons_count'],
        SQLITE_QUERIES_PATH['get_movies_between'],
        PSQL_QUERIES_PATH['insert_into_film_work_persons'],
        prepare_psql_film_work_person_data,
        load_step=500,
    )


if __name__ == '__main__':
    main()
