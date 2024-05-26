INSERT INTO content.genre (
    id, 
    name,
    description,
    created_at, 
    updated_at
    ) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        description = EXCLUDED.description,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at;