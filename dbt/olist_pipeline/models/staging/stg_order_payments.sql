{{ config(materialized='view') }}
SELECT
    order_id,
    payment_sequential,
    payment_type,
    CAST(payment_installments AS INT64) AS payment_installments,
    CAST(payment_value AS FLOAT64)      AS payment_value
FROM {{ source('raw', 'order_payments') }}
WHERE order_id IS NOT NULL
  AND payment_value > 0