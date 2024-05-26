import uuid
from datetime import datetime

from scripts.sqlite_to_postgres.models.psql_models import FilmType, Film


def prepare_psql_movies_data(movies: list[tuple]) -> list[Film]:
    new_movies = []
    
    for movie in movies:
        movie_data = {
            'id': str(uuid.uuid4),
            'title': movie[5],
            'description': movie[6],
            'creation_date': None,
            'certificate': None,
            'file_path': None,
            'rating': movie[6],
            'type': FilmType.movie,
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        new_movies.append(
            Film(**movie_data),
        )
    
    return new_movies
        
