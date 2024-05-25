INSERT INTO content.film_work (
        id,
        title,
        description,
        creation_date,
        certificate,
        file_path,
        rating,
        type,
        created_at,
        updated_at,
    )
VALUES %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW();