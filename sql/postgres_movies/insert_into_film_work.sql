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
    updated_at
    ) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT (id) DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        creation_date = EXCLUDED.creation_date,
        certificate = EXCLUDED.certificate,
        file_path = EXCLUDED.file_path,
        rating = EXCLUDED.rating,
        type = EXCLUDED.type,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at;