{{ config(materialized='table') }}
SELECT
    TO_HEX(MD5(seller_id))  AS seller_sk,
    seller_id,
    seller_zip_prefix,
    seller_city,
    seller_state
FROM {{ ref('stg_sellers') }}