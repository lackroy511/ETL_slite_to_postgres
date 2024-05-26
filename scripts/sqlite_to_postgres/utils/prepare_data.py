import uuid
from datetime import datetime

from scripts.sqlite_to_postgres.models.psql_models import FilmType, Film


def prepare_psql_movies_data(movies: list[tuple]) -> list[Film]:
    new_movies = []
    
    for movie in movies:
        movie_data = {
            'id': str(uuid.uuid4()),
            'title': movie[5],
            'description': movie[6],
            'creation_date': None,
            'certificate': None,
            'file_path': None,
            'rating': float(movie[8]) if movie[8] else None,
            'type': 'movie',
        }
        new_movies.append(
            tuple(dict(Film(**movie_data)).values()),
        )
    
    return new_movies
        
