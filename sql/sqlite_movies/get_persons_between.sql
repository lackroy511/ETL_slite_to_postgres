SELECT *
FROM (
    SELECT DISTINCT ROW_NUMBER() OVER (ORDER BY movies.director, writers.name, actors.name) as row_number, 
        movies.director, 
        writers.name as writer, 
        actors.name as actor
    FROM movies
    LEFT JOIN writers ON movies.writer = writers.id
    LEFT JOIN movie_actors ON movies.id = movie_actors.movie_id
    LEFT JOIN actors ON movie_actors.actor_id = actors.id
    ) AS movies_with_row_number
WHERE row_number BETWEEN FROM_ROW and TO_ROW;
