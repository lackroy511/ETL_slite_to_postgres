SELECT COUNT(*)
FROM (
        WITH movies_with_row_number AS (
        SELECT DISTINCT 
            ROW_NUMBER() OVER (ORDER BY movies.director, writers.name, actors.name) as row_number, 
            movies.director, 
            writers.name as writer, 
            actors.name as actor
        FROM movies
        LEFT JOIN writers ON movies.writer = writers.id
        LEFT JOIN movie_actors ON movies.id = movie_actors.movie_id
        LEFT JOIN actors ON movie_actors.actor_id = actors.id
    ),

    all_writers AS (
        SELECT 
            NULL as row_number, 
            NULL as director, 
            name as writer, 
            NULL as actor
        FROM writers
    ),

    combined_results AS (
        SELECT * FROM movies_with_row_number
        UNION ALL
        SELECT * FROM all_writers
    )

    SELECT 
        ROW_NUMBER() OVER (ORDER BY director, writer, actor) as row_number, 
        director, 
        writer, 
        actor
    FROM combined_results) AS combined_results;