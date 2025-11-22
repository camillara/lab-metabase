{{ config(materialized = 'view') }}

WITH northwind AS (

    SELECT
        source_system,
        source_customer_id,
        company_name,
        contact_name,
        address,
        city,
        TRIM(region)                 AS state_region,
        country,
        phone,
        zip,
        email,
        source_channel,
        created_at
    FROM {{ ref('stg_northwind_customers') }}

),

metabase AS (

    SELECT
        source_system,
        source_customer_id,
        full_name                     AS company_name,
        full_name                     AS contact_name,
        address,
        city,
        TRIM(state)                   AS state_region,
        country,
        phone,
        zip,
        email,
        source_channel,
        created_at
    FROM {{ ref('stg_metabase_people') }}

),

unioned AS (

    SELECT * FROM northwind
    UNION ALL
    SELECT * FROM metabase

)

SELECT
    md5(source_system || '-' || source_customer_id) AS cliente_sk,
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
FROM unioned

