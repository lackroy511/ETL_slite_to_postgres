import psycopg2
from psycopg2.extras import execute_batch


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
    def init_schema(self, query_path: str) -> None:
        with open(query_path, 'r') as file:
            query = file.read()

        self.cursor.execute(query)

    def insert_entities(
            self, 
            query_path: str, 
            data: list[list], 
            page_size: int = 5000) -> None:
        
        with open(query_path, 'r') as file:
            query = file.read()

        execute_batch(
            self.cursor,
            query,
            data,
            page_size=page_size,
        )
