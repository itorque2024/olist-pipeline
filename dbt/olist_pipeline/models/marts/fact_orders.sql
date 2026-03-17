{{ config(
    materialized='table',
    partition_by={'field': 'order_purchase_date', 'data_type': 'date'}
) }}

WITH payments AS (
    SELECT
        order_id,
        SUM(payment_value)                     AS payment_value,
        MAX(payment_installments)              AS payment_installments,
        STRING_AGG(DISTINCT payment_type, ', ') AS payment_types
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),
reviews AS (
    SELECT order_id, review_score, review_date
    FROM {{ ref('stg_order_reviews') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id ORDER BY review_date DESC
    ) = 1
)
SELECT
    -- Surrogate key
    TO_HEX(MD5(CONCAT(i.order_id,
        CAST(i.order_item_id AS STRING))))      AS order_item_sk,

    -- Natural keys
    i.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,

    -- Date FK
    CAST(FORMAT_DATE('%Y%m%d',
        o.order_purchase_date) AS INT64)        AS date_sk,
    o.order_purchase_date,
    o.order_purchase_ts,
    o.delivered_date,
    o.estimated_delivery_date,

    -- Measures
    i.price,
    i.freight_value,
    i.total_item_amount                         AS total_sale_amount,
    p.payment_value,
    p.payment_installments,
    p.payment_types                             AS payment_type,

    -- Review
    r.review_score,
    r.review_date,

    -- Status
    o.order_status,

    -- ── DERIVED COLUMNS (rubric requirement) ──────────────
    DATE_DIFF(o.delivered_date,
        o.order_purchase_date, DAY)             AS delivery_days,
    DATE_DIFF(o.estimated_delivery_date,
        o.order_purchase_date, DAY)             AS estimated_delivery_days,
    CASE WHEN o.delivered_date > o.estimated_delivery_date
         THEN TRUE ELSE FALSE END               AS is_late_delivery,
    CASE WHEN i.price > 200
         THEN TRUE ELSE FALSE END               AS is_high_value

FROM {{ ref('stg_order_items') }}  i
JOIN {{ ref('stg_orders') }}       o  ON i.order_id = o.order_id
LEFT JOIN payments                 p  ON i.order_id = p.order_id
LEFT JOIN reviews                  r  ON i.order_id = r.order_id