{{ config(materialized = 'view') }}

WITH order_lines AS (

    -- Cada linha representa 1 item do pedido
    SELECT
        od.order_id::text              AS source_order_id,
        od.product_id::text            AS source_product_id,
        od.unit_price::numeric(12,2)   AS unit_price,
        od.quantity::int               AS quantity,
        od.discount::numeric(12,4)     AS discount
    FROM ext_northwind.order_details od
),

order_header AS (

    -- Cabeçalho do pedido (uma linha por pedido)
    SELECT
        o.order_id::text               AS source_order_id,
        o.customer_id::text            AS source_customer_id,
        o.order_date                   AS order_date,
        o.shipped_date                 AS shipped_date,
        o.freight::numeric(12,2)       AS freight_amount,
        'northwind'                    AS source_system
    FROM ext_northwind.orders o
),

joined AS (

    SELECT
        ol.source_order_id,
        oh.source_customer_id,
        ol.source_product_id,
        ol.unit_price,
        ol.quantity,

        -- Discount no Northwind é FRAÇÃO (ex: 0.10 = 10%)
        (ol.unit_price * ol.quantity * ol.discount)::numeric(12,2)
            AS discount_amount,

        -- Total do item já considerando desconto
        (ol.unit_price * ol.quantity * (1 - ol.discount))::numeric(12,2)
            AS total_amount,

        -- Subtotal antes do desconto
        (ol.unit_price * ol.quantity)::numeric(12,2)
            AS subtotal_amount,

        oh.order_date,
        oh.shipped_date,
        oh.freight_amount,
        oh.source_system
    FROM order_lines ol
    LEFT JOIN order_header oh
      ON ol.source_order_id = oh.source_order_id
)

SELECT
    source_order_id,
    source_customer_id,
    source_product_id,
    quantity,
    unit_price,
    subtotal_amount,
    discount_amount,
    total_amount,
    order_date,
    shipped_date,
    freight_amount,
    source_system
FROM joined
