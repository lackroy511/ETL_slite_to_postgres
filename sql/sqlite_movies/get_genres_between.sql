SELECT *
FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY id) as row_number, genre
    FROM movies
    ) AS movies_with_row_number
WHERE row_number BETWEEN FROM_ROW and TO_ROW;