{{ config(materialized = 'table') }}

WITH dates AS (
    SELECT
        d::date AS full_date
    FROM generate_series(
        '1990-01-01'::date,   -- início (cobre Northwind tranquilamente)
        '2030-12-31'::date,   -- fim (cobre Metabase sample e futuro)
        interval '1 day'
    ) AS d
)

SELECT
    -- chave substituta (formato YYYYMMDD)
    TO_CHAR(full_date, 'YYYYMMDD')::int AS date_key,

    -- data completa
    full_date,

    -- componentes da data
    EXTRACT(YEAR  FROM full_date)::int AS year,
    EXTRACT(MONTH FROM full_date)::int AS month,
    EXTRACT(DAY   FROM full_date)::int AS day,

    -- nome do mês e do dia
    TO_CHAR(full_date, 'Month')         AS month_name,
    TO_CHAR(full_date, 'Mon')           AS month_name_short,
    TO_CHAR(full_date, 'Dy')            AS weekday_short,
    EXTRACT(ISODOW FROM full_date)::int AS weekday_iso,   -- 1=segunda ... 7=domingo

    -- flags úteis
    CASE WHEN EXTRACT(ISODOW FROM full_date) IN (6, 7)
         THEN TRUE ELSE FALSE END       AS is_weekend,

    -- ano-mês e ano-trimestre
    TO_CHAR(full_date, 'YYYY-MM')       AS year_month,
    TO_CHAR(full_date, 'YYYY"Q"Q')      AS year_quarter
FROM dates
ORDER BY full_date
