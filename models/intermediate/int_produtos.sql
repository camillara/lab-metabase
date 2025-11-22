{{ config(materialized = 'view') }}

WITH northwind AS (

    SELECT
        source_product_id::text              AS source_product_id,
        product_name                         AS product_name,
        NULL::varchar                        AS ean,
        vendor_name                          AS vendor_name,
        category_name                        AS category_name,
        unit_price                           AS price,
        NULL::numeric(4,2)                   AS rating,
        source_system                        AS source_system,
        CURRENT_TIMESTAMP                    AS created_at
    FROM {{ ref('stg_northwind_products') }}

),

metabase AS (

    SELECT
        source_product_id::text              AS source_product_id,
        product_name,
        ean,
        vendor_name,
        category_name,
        price,
        rating,
        source_system,
        created_at
    FROM {{ ref('stg_metabase_products') }}

),

unioned AS (

    SELECT
        source_product_id,
        product_name,
        ean,
        vendor_name,
        category_name,
        price,
        rating,
        source_system,
        created_at
    FROM northwind

    UNION ALL

    SELECT
        source_product_id,
        product_name,
        ean,
        vendor_name,
        category_name,
        price,
        rating,
        source_system,
        created_at
    FROM metabase
)

SELECT
    source_product_id,
    product_name,
    ean,
    vendor_name,
    category_name,
    price,
    rating,
    source_system,
    created_at,
    md5(source_product_id || '-' || source_system) AS produto_sk
FROM unioned

