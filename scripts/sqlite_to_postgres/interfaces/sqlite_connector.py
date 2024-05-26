import sqlite3


SQLITE_QUERIES_PATH = {
    'get_movies_count': 'sql/sqlite_movies/get_movies_count.sql',
    'get_movies_between': 'sql/sqlite_movies/get_movies_between.sql',
    'get_genres': 'sql/sqlite_movies/get_genres.sql',
}


class SQLiteConnector:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()


class SQLiteMoviesDB(SQLiteConnector):
    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)

    def get_movies_count(self) -> int:
        with open(SQLITE_QUERIES_PATH['get_movies_count'], 'r') as file:
            query = file.read()

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_movies(self, from_row: int, to_row: int) -> list[tuple]:
        with open(SQLITE_QUERIES_PATH['get_movies_between'], 'r') as file:
            query = file.read()
            query = query.replace('FROM_ROW', str(from_row))
            query = query.replace('TO_ROW', str(to_row))

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_genres(self) -> list[tuple]:
        with open(SQLITE_QUERIES_PATH['get_genres'], 'r') as file:
            query = file.read()

        self.cursor.execute(query)
        return self.cursor.fetchall()
