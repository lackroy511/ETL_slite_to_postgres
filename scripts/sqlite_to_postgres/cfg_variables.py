import os

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
    'insert_into_film_work_genre': 'sql/postgres_movies/insert_into_film_work_genres.sql',
    'insert_into_film_work_persons': '/root/project/sql/postgres_movies/insert_into_film_work_persons.sql',
}
SQLITE_DB_PATH = 'databases/movies.sqlite'
PSQL_DSN = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT'),
}
