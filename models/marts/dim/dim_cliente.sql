{{ config(materialized = 'table') }}

SELECT
    cliente_sk          AS cliente_key,
    source_system,
    source_customer_id,
    company_name,
    contact_name,
    address,
    city,
    state_region,
    country,
    phone,
    zip,
    email,
    source_channel,
    created_at
FROM {{ ref('int_clientes') }}

