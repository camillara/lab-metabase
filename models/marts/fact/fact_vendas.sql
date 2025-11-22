{{ config(materialized = 'table') }}

WITH vendas AS (
    SELECT *
    FROM {{ ref('int_vendas') }}
),

dim_cliente AS (
    SELECT
        cliente_key,
        source_system,
        source_customer_id
    FROM {{ ref('dim_cliente') }}
),

dim_produto AS (
    SELECT
        produto_sk,
        source_system,
        source_product_id
    FROM {{ ref('dim_produto') }}
),

joined AS (

    SELECT
        md5(
            vendas.source_system || '-' ||
            vendas.source_order_id || '-' ||
            vendas.source_product_id
        )                                      AS venda_sk,

        -- Dimensão Cliente
        dim_cliente.cliente_key                AS cliente_key,

        -- Dimensão Produto
        dim_produto.produto_sk                 AS produto_key,

        -- Natural Keys
        vendas.source_system,
        vendas.source_order_id,
        vendas.source_customer_id,
        vendas.source_product_id,

        -- Datas
        vendas.order_datetime,
        CAST(vendas.order_datetime AS date)        AS order_date,
        vendas.shipped_date,

        TO_CHAR(
            CAST(vendas.order_datetime AS date),
            'YYYYMMDD'
        )::int                                     AS order_date_key,

        -- Métricas
        vendas.quantity,
        vendas.subtotal_amount,
        vendas.discount_amount,
        vendas.tax_amount,
        vendas.total_amount,
        vendas.freight_amount
    FROM vendas
    LEFT JOIN dim_cliente
        ON dim_cliente.source_system      = vendas.source_system
       AND dim_cliente.source_customer_id = vendas.source_customer_id
    LEFT JOIN dim_produto
        ON dim_produto.source_system      = vendas.source_system
       AND dim_produto.source_product_id  = vendas.source_product_id
)

SELECT
    venda_sk,
    cliente_key,
    produto_key,
    source_system,
    source_order_id,
    source_customer_id,
    source_product_id,
    order_datetime,
    order_date,
    order_date_key,
    shipped_date,
    quantity,
    subtotal_amount,
    discount_amount,
    tax_amount,
    total_amount,
    freight_amount
FROM joined
