{{ config(materialized='view') }}
SELECT
    review_id,
    order_id,
    CAST(review_score AS INT64)    AS review_score,
    DATE(review_creation_date)     AS review_date,
    DATE(review_answer_timestamp)  AS review_answer_date
FROM {{ source('raw', 'order_reviews') }}
WHERE order_id IS NOT NULL
  AND review_score BETWEEN 1 AND 5