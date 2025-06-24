CREATE TABLE actors (
    actorid STRING PRIMARY KEY,
    actor STRING,
    films ARRAY<STRUCT<
        film STRING,
        votes INT,
        rating FLOAT,
        filmid STRING
    >>,
    quality_class STRING, -- ENUM: 'star', 'good', 'average', 'bad'
    is_active BOOLEAN
);
INSERT INTO actors
SELECT
    actorid,
    ANY_VALUE(actor) AS actor,
    ARRAY_AGG(STRUCT(film, votes, rating, filmid)) AS films,
    CASE
        WHEN AVG(rating) OVER (PARTITION BY actorid ORDER BY year DESC ROWS BETWEEN CURRENT ROW AND CURRENT ROW) > 8 THEN 'star'
        WHEN AVG(rating) > 7 THEN 'good'
        WHEN AVG(rating) > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    MAX(CASE WHEN year = 2000 THEN TRUE ELSE FALSE END) AS is_active
FROM actor_films_raw
WHERE year <= 2000
GROUP BY actorid;

CREATE TABLE actors_history_scd (
    actorid STRING,
    actor STRING,
    quality_class STRING,
    is_active BOOLEAN,
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (actorid, start_date)
);
INSERT INTO actors_history_scd
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    DATE_FROM_UNIX_DATE(MIN(year) * 365) AS start_date,
    DATE_FROM_UNIX_DATE(MAX(year) * 365) AS end_date
FROM (
    SELECT
        actorid,
        ANY_VALUE(actor) AS actor,
        CASE
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 THEN 'good'
            WHEN AVG(rating) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        MAX(CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN TRUE ELSE FALSE END) AS is_active,
        year
    FROM actor_films_raw
    GROUP BY actorid, year
)
GROUP BY actorid, actor, quality_class, is_active;

