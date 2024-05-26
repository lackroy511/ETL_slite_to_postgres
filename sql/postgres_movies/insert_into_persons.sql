INSERT INTO content.person (
    id, 
    full_name,
    birth_date,
    created_at, 
    updated_at
) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (full_name) DO UPDATE SET
    birth_date = EXCLUDED.birth_date;