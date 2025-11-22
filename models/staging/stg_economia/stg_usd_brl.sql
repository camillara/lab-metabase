{{ config(materialized='view') }}

WITH raw AS (
    SELECT
        -- agora usamos os nomes REAIS da tabela dw_ext_economia.usd_brl_raw
        data_text,
        taxa_text
    FROM {{ source('ext_economia', 'usd_brl_raw') }}
),

cleaned AS (
    SELECT
        -- converte "19/11/2025" -> date
        TO_DATE(data_text, 'DD/MM/YYYY') AS data,

        -- converte "5,334" -> 5.334
        REPLACE(taxa_text, ',', '.')::numeric(10,4) AS taxa_cambio
    FROM raw
    WHERE data_text IS NOT NULL
      AND taxa_text IS NOT NULL
)

SELECT
    data,
    taxa_cambio
FROM cleaned
ORDER BY data DESC
