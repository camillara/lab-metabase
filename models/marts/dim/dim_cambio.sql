{{ config(materialized='table') }}

WITH fonte AS (

    SELECT
        data,
        taxa_cambio
    FROM {{ ref('stg_usd_brl') }}

)

SELECT
    -- chave substituta simples baseada na data
    md5(data::text)          AS cambio_key,
    data,
    taxa_cambio
FROM fonte
ORDER BY data