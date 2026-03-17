{{ config(materialized='view') }}
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    TIMESTAMP(shipping_limit_date)  AS shipping_limit_ts,
    CAST(price AS FLOAT64)          AS price,
    CAST(freight_value AS FLOAT64)  AS freight_value,
    ROUND(CAST(price AS FLOAT64)
        + CAST(freight_value AS FLOAT64), 2) AS total_item_amount
FROM {{ source('raw', 'order_items') }}
WHERE order_id IS NOT NULL