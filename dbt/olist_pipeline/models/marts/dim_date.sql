{{ config(materialized='table') }}
WITH date_spine AS (
    SELECT DATE_ADD(DATE '2016-01-01', INTERVAL n DAY) AS full_date
    FROM UNNEST(GENERATE_ARRAY(0, 1460)) AS n
)
SELECT
    CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64) AS date_sk,
    full_date,
    EXTRACT(YEAR    FROM full_date) AS year,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(MONTH   FROM full_date) AS month,
    FORMAT_DATE('%B', full_date)    AS month_name,
    EXTRACT(WEEK    FROM full_date) AS week_of_year,
    FORMAT_DATE('%A', full_date)    AS day_of_week,
    CASE WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1,7)
         THEN TRUE ELSE FALSE END   AS is_weekend
FROM date_spine