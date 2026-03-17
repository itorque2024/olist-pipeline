{{ config(materialized='view') }}
SELECT
    order_id,
    customer_id,
    order_status,
    TIMESTAMP(order_purchase_timestamp)        AS order_purchase_ts,
    DATE(order_purchase_timestamp)             AS order_purchase_date,
    TIMESTAMP(order_approved_at)               AS order_approved_ts,
    TIMESTAMP(order_delivered_customer_date)   AS delivered_customer_ts,
    DATE(order_delivered_customer_date)        AS delivered_date,
    DATE(order_estimated_delivery_date)        AS estimated_delivery_date,
    CURRENT_TIMESTAMP()                        AS ingested_at
FROM {{ source('raw', 'orders') }}
WHERE order_id IS NOT NULL