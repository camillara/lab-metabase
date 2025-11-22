{{ config(materialized = 'view') }}

WITH metabase AS (

    SELECT
        source_system,
        source_order_id,
        source_customer_id,
        source_product_id,
        order_datetime,
        NULL::date                                 AS shipped_date,
        quantity,
        subtotal_amount,
        discount_amount,
        tax_amount,
        total_amount,
        NULL::numeric(12,2)                        AS freight_amount
    FROM {{ ref('stg_metabase_orders') }}

),

northwind AS (

    SELECT
        source_system,
        source_order_id,
        source_customer_id,
        source_product_id,
        order_date::timestamp                      AS order_datetime,
        shipped_date,
        quantity,
        subtotal_amount,
        discount_amount,
        NULL::numeric(12,2)                        AS tax_amount,
        total_amount,
        freight_amount
    FROM {{ ref('stg_northwind_orders') }}

),

unioned AS (
    SELECT * FROM metabase
    UNION ALL
    SELECT * FROM northwind
)

SELECT
    source_system,
    source_order_id,
    source_customer_id,
    source_product_id,
    order_datetime,
    shipped_date,
    quantity,
    subtotal_amount,
    discount_amount,
    tax_amount,
    total_amount,
    freight_amount
FROM unioned
