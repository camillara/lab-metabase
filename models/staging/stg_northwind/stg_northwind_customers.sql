{{ config(materialized = 'view') }}

WITH source AS (
    SELECT
        c.customer_id::text                 AS source_customer_id,
        UPPER(TRIM(c.company_name))         AS company_name,
        UPPER(TRIM(c.contact_name))         AS contact_name,
        UPPER(TRIM(c.address))              AS address,
        UPPER(TRIM(c.city))                 AS city,
        UPPER(TRIM(COALESCE(c.region, ''))) AS region,
        NULL::varchar                       AS state,
        UPPER(TRIM(c.country))              AS country,
        TRIM(c.postal_code)                 AS zip,
        TRIM(c.phone)                       AS phone,
        NULL::varchar                       AS email,
        'northwind'                         AS source_system,
        'NORTHWIND'                         AS source_channel,
        NULL::timestamp                     AS created_at
    FROM ext_northwind.customers c
)

SELECT
    source_customer_id,
    company_name,
    contact_name,
    address,
    city,
    region,
    state,
    country,
    phone,
    zip,
    email,
    source_system,
    source_channel,
    created_at
FROM source

