{{ config(materialized = 'view') }}

WITH source AS (
    SELECT
        p.id::text              AS source_customer_id,
        UPPER(TRIM(p.name))     AS full_name,
        UPPER(TRIM(p.address))  AS address,
        UPPER(TRIM(p.city))     AS city,
        NULL::varchar           AS region,
        p.state                 AS state,
        'USA'                   AS country,
        TRIM(p.zip)             AS zip,
        NULL::varchar           AS phone,
        TRIM(p.email)           AS email,
        UPPER(TRIM(p.source))   AS source_channel,
        p.created_at            AS created_at,
        'metabase'              AS source_system
    FROM ext_metabase.people p
)

SELECT
    source_customer_id,
    full_name,
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
