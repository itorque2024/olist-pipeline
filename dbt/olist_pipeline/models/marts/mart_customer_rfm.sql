{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        MAX(f.order_purchase_date)              AS last_order_date,
        COUNT(DISTINCT f.order_id)              AS frequency,
        ROUND(SUM(f.total_sale_amount), 2)      AS monetary,
        ROUND(AVG(f.total_sale_amount), 2)      AS avg_order_value,
        ROUND(AVG(f.review_score), 2)           AS avg_review_score,
        c.customer_state
    FROM {{ ref('fact_orders') }}   f
    JOIN {{ ref('dim_customer') }}  c  ON f.customer_id = c.customer_id
    WHERE f.order_status = 'delivered'
    GROUP BY c.customer_unique_id, c.customer_state
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.customer_unique_id ORDER BY MAX(f.order_purchase_date) DESC) = 1
),
rfm AS (
    SELECT *,
        DATE_DIFF(DATE '2018-10-31',
            last_order_date, DAY)               AS recency_days,
        monetary                                AS customer_ltv
    FROM customer_orders
)
SELECT
    customer_unique_id,
    last_order_date,
    recency_days,
    frequency,
    monetary,
    avg_order_value,
    avg_review_score,
    customer_ltv,
    customer_state,
    CASE
        WHEN frequency >= 3
         AND recency_days <= 180
         AND monetary    >= 500  THEN 'VIP'
        WHEN frequency >= 2
         AND recency_days <= 365  THEN 'Loyal'
        WHEN monetary >= 500
         AND frequency = 1        THEN 'Big Spender'
        WHEN recency_days > 365   THEN 'At-Risk'
        ELSE 'One-Timer'
    END AS rfm_segment
FROM rfm