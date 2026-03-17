{{ config(materialized='view') }}
SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english,
             p.product_category_name)          AS product_category_english,
    CAST(p.product_weight_g    AS INT64)       AS product_weight_g,
    CAST(p.product_length_cm   AS INT64)       AS product_length_cm,
    CAST(p.product_height_cm   AS INT64)       AS product_height_cm,
    CAST(p.product_width_cm    AS INT64)       AS product_width_cm,
    CAST(p.product_photos_qty  AS INT64)       AS product_photos_qty,
    CAST(p.product_length_cm AS INT64)
      * CAST(p.product_height_cm AS INT64)
      * CAST(p.product_width_cm  AS INT64)     AS volume_cm3
FROM {{ source('raw', 'products') }} p
LEFT JOIN {{ source('raw', 'category_translation') }} t
    ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL