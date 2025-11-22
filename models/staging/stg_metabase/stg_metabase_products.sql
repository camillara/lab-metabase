{{ config(materialized = 'view') }}

WITH source AS (
    SELECT
        p.id::text                          AS source_product_id,
        TRIM(p.ean)                         AS ean,
        UPPER(TRIM(p.title))                AS product_name,
        UPPER(TRIM(p.category))             AS category_name,
        UPPER(TRIM(p.vendor))               AS vendor_name,
        p.price::numeric(12,2)              AS price,
        p.rating::numeric(4,2)              AS rating,
        p.created_at                        AS created_at,
        'metabase'                          AS source_system
    FROM ext_metabase.products p
)

SELECT
    source_product_id,
    ean,
    product_name,
    category_name,
    vendor_name,
    price,
    rating,
    created_at,
    source_system
FROM source
