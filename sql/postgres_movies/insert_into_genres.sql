INSERT INTO content.genre (
    id, 
    name,
    description,
    created_at, 
    updated_at
) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description;
