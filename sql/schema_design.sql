CREATE SCHEMA IF NOT EXISTS content;

-- Основные таблицы
CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    certificate TEXT,
    file_path TEXT,
    rating FLOAT,
    type TEXT not null,
    sqlite_id VARCHAR(255) UNIQUE NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre(
    id uuid PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person(
    id uuid PRIMARY KEY,
    full_name TEXT UNIQUE NOT NULL,
    birth_date DATE,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

-- m2m Таблицы
CREATE TABLE IF NOT EXISTS content.film_work_genre(
    id uuid PRIMARY KEY,
    film_work_id uuid REFERENCES content.film_work(id),
    genre_id uuid REFERENCES content.genre(id),
    created_at timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.film_work_person(
    id uuid PRIMARY KEY,
    film_work_id uuid REFERENCES content.film_work(id),
    person_id uuid REFERENCES content.person(id),
    role TEXT NOT NULL,
    created_at timestamp with time zone
);

-- Индексы

CREATE UNIQUE INDEX IF NOT EXISTS film_work_genre_index
ON content.film_work_genre (film_work_id, genre_id);

CREATE UNIQUE INDEX IF NOT EXISTS film_work_person_role_index
ON content.film_work_person (film_work_id, person_id, role);

SELECT * FROM content.film_work
WHERE title LIKE 'Star Trek'