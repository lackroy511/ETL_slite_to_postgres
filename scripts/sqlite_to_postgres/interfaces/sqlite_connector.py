import sqlite3

SQLITE_QUERIES_PATH = {
    'get_movies_count': 'sql/sqlite_movies/get_movies_count.sql',
    'get_movies_between': 'sql/sqlite_movies/get_movies_between.sql',
    'get_genres_between': 'sql/sqlite_movies/get_genres_between.sql',
    'get_persons_count': 'sql/sqlite_movies/get_persons_count.sql',
    'get_persons_between': 'sql/sqlite_movies/get_persons_between.sql',
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

    def get_entities_count(self, query_path) -> int:
        with open(query_path, 'r') as file:
            query = file.read()

        self.cursor.execute(query)
        return self.cursor.fetchall()[0][0]

    def get_entities_in_range(self, query_path, from_row: int, to_row: int) -> list[tuple]:
        with open(query_path, 'r') as file:
            query = file.read()
            query = query.replace('FROM_ROW', str(from_row))
            query = query.replace('TO_ROW', str(to_row))

        self.cursor.execute(query)
        return self.cursor.fetchall()
