SELECT DISTINCT COUNT(*)
FROM movies
LEFT JOIN writers ON movies.writer = writers.id
LEFT JOIN movie_actors ON movies.id = movie_actors.movie_id
LEFT JOIN actors ON movie_actors.actor_id = actors.id;