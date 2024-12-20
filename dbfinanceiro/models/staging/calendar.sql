WITH date_range AS (
    SELECT 
        date_trunc('day', '{{ var("start_date", "2020-01-01") }}'::date) AS start_date,
        date_trunc('day', '{{ var("end_date", "2026-12-31") }}'::date) AS end_date
),
dates AS (
    SELECT 
        start_date + (n * interval '1 day') AS calendar_date
    FROM 
        date_range, 
        generate_series(0, DATE_PART('day', end_date - start_date)::int) AS n
)
SELECT 
    calendar_date AS date,
    EXTRACT(YEAR FROM calendar_date) AS year,
    EXTRACT(MONTH FROM calendar_date) AS month,
    EXTRACT(DAY FROM calendar_date) AS day,
    TO_CHAR(calendar_date, 'Day') AS day_name,
    EXTRACT(ISODOW FROM calendar_date) AS iso_weekday,
    CASE WHEN EXTRACT(ISODOW FROM calendar_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    EXTRACT(WEEK FROM calendar_date) AS week,
    EXTRACT(QUARTER FROM calendar_date) AS quarter
FROM 
    dates
