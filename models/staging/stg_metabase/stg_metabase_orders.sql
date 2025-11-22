{{ config(materialized = 'view') }}

WITH source AS (
    SELECT
        o.id::text                      AS source_order_id,       -- id do pedido na origem
        o.user_id::text                 AS source_customer_id,    -- liga com dim_cliente (people.id)
        o.product_id::text              AS source_product_id,     -- liga com dim_produto (products.id)
        o.quantity::int                 AS quantity,
        o.subtotal::numeric(12,2)       AS subtotal_amount,
        o.tax::numeric(12,2)            AS tax_amount,
        o.total::numeric(12,2)          AS total_amount,
        o.discount::numeric(12,2)       AS discount_amount,
        o.created_at                    AS order_datetime,
        'metabase'                      AS source_system
    FROM ext_metabase.orders o
)

SELECT
    source_order_id,
    source_customer_id,
    source_product_id,
    quantity,
    subtotal_amount,
    tax_amount,
    total_amount,
    discount_amount,
    order_datetime,
    source_system
FROM source
