{{ config(materialized='table') }}
SELECT
    TO_HEX(MD5(product_id))  AS product_sk,
    product_id,
    product_category_name,
    product_category_english,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty,
    volume_cm3
FROM {{ ref('stg_products') }}