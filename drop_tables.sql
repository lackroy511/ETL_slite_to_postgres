DROP TABLE content.film_work CASCADE;
DROP TABLE content.genre CASCADE;
DROP TABLE content.person CASCADE;
CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    certificate TEXT,
    file_path TEXT,
    rating FLOAT,
    type TEXT not null,
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

SELECT COUNT(*) FROM content.film_work;
SELECT COUNT(*) FROM content.genre;
SELECT COUNT(*) FROM content.person;
