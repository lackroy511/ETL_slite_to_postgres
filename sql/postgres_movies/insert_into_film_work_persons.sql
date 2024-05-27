INSERT INTO content.film_work_person (
    id, 
    film_work_id,
    person_id,
    role,
    created_at
) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (film_work_id, person_id, role) DO NOTHING;