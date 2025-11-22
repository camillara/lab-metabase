{{ config(materialized = 'table') }}

WITH base AS (

    SELECT
        f.cliente_key,
        c.company_name,
        c.state_region,
        c.country,

        t.date_key,
        t.full_date,
        t.year,
        t.month,
        t.year_month,

        f.source_system,
        f.source_order_id,
        f.quantity,
        f.total_amount
    FROM {{ ref('fact_vendas') }} f
    JOIN {{ ref('dim_cliente') }} c
      ON c.cliente_key = f.cliente_key
    JOIN {{ ref('dim_tempo') }} t
      ON t.date_key = f.order_date_key
),

agregado AS (

    SELECT
        cliente_key,
        company_name,
        state_region,
        country,
        year,
        month,
        year_month,
        source_system,

        COUNT(DISTINCT source_order_id)              AS qtde_pedidos,
        SUM(quantity)                                AS qtde_itens,
        SUM(total_amount)                            AS valor_total,

        CASE
            WHEN COUNT(DISTINCT source_order_id) > 0
            THEN SUM(total_amount) / COUNT(DISTINCT source_order_id)
            ELSE 0
        END                                          AS ticket_medio
    FROM base
    GROUP BY
        cliente_key,
        company_name,
        state_region,
        country,
        year,
        month,
        year_month,
        source_system
)

SELECT
    -- chave substituta da linha do DM (cliente + mês + origem)
    md5(
        cliente_key || '-' ||
        year_month  || '-' ||
        COALESCE(source_system, '')
    )                                AS cliente_resumo_sk,

    cliente_key,
    company_name,
    state_region,
    country,
    year,
    month,
    year_month,
    source_system,
    qtde_pedidos,
    qtde_itens,
    valor_total,
    ticket_medio
FROM agregado
ORDER BY
    year, month, company_name
