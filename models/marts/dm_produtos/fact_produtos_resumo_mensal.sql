{{ config(materialized='table') }}

WITH base AS (

    SELECT
        f.produto_key,
        p.product_name,
        p.category_name,
        p.vendor_name,

        t.date_key,
        t.full_date,
        t.year,
        t.month,
        t.year_month,

        f.source_system,
        f.source_order_id,
        f.source_product_id,
        f.cliente_key,
        f.quantity,
        f.total_amount
    FROM {{ ref('fact_vendas') }} f
    JOIN {{ ref('dim_produto') }} p
      ON p.produto_sk = f.produto_key
    JOIN {{ ref('dim_tempo') }} t
      ON t.date_key = f.order_date_key
),

agregado AS (

    SELECT
        produto_key,
        product_name,
        category_name,
        vendor_name,
        year,
        month,
        year_month,
        source_system,

        SUM(quantity)                         AS qtde_vendida,
        SUM(total_amount)                     AS valor_total,

        COUNT(DISTINCT cliente_key)           AS clientes_unicos,

        CASE WHEN SUM(quantity) > 0
             THEN SUM(total_amount) / SUM(quantity)
             ELSE 0
        END                                   AS ticket_medio
    FROM base
    GROUP BY
        produto_key,
        product_name,
        category_name,
        vendor_name,
        year,
        month,
        year_month,
        source_system
)

SELECT
    md5(
        produto_key || '-' ||
        year_month  || '-' ||
        COALESCE(source_system, '')
    )                                  AS produto_resumo_sk,

    produto_key,
    product_name,
    category_name,
    vendor_name,
    year,
    month,
    year_month,
    source_system,
    qtde_vendida,
    valor_total,
    clientes_unicos,
    ticket_medio

FROM agregado
ORDER BY year, month, product_name
