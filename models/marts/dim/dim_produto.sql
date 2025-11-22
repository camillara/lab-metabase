{{ config(materialized = 'table') }}

WITH produtos AS (

    SELECT
        produto_sk,
        source_product_id,
        source_system,
        product_name,
        category_name,
        vendor_name,
        price,
        rating,
        ean,
        created_at
    FROM {{ ref('int_produtos') }}

),

dedup AS (
    SELECT
        produto_sk,
        source_product_id,
        source_system,
        product_name,
        category_name,
        vendor_name,
        price,
        rating,
        ean,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY produto_sk
            ORDER BY created_at DESC
        ) AS rn
    FROM produtos
)

SELECT
    produto_sk,
    source_product_id,
    source_system,
    product_name,
    category_name,
    vendor_name,
    price,
    rating,
    ean,
    created_at
FROM dedup
WHERE rn = 1
