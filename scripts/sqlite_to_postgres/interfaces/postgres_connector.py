import psycopg2
from psycopg2.extras import execute_batch


PSQL_QUERIES_PATH = {
    'schema_design': 'sql/schema_design.sql',
    'insert_into_film_work': 'sql/postgres_movies/insert_into_film_work.sql',
}


class PostgreSQLConnector:
    def __init__(
            self,
            dbname: str,
            user: str,
            password: str,
            host: str,
            port: str) -> None:

        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()


class PostgreSQLMoviesDB(PostgreSQLConnector):
    def init_schema(self):
        with open(PSQL_QUERIES_PATH['schema_design'], 'r') as file:
            query = file.read()

        self.cursor.execute(query)

    def insert_to_film_work(self, data: list[tuple], page_size: int = 5000) -> None:
        with open(PSQL_QUERIES_PATH['insert_into_film_work'], 'r') as file:
            query = file.read()

        execute_batch(
            self.cursor,
            query,
            data,
            page_size=page_size,
        )
