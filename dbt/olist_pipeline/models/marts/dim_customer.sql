{{ config(materialized='table') }}
SELECT
    TO_HEX(MD5(customer_id)) AS customer_sk,
    customer_id,
    customer_unique_id,
    customer_zip_prefix,
    customer_city,
    customer_state
FROM {{ ref('stg_customers') }}