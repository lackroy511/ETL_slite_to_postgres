INSERT INTO content.film_work_genre (
    id, 
    film_work_id,
    genre_id,
    created_at
) 
VALUES (%s, %s, %s, %s)
ON CONFLICT (film_work_id, genre_id) DO NOTHING;