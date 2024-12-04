WITH source AS (
    SELECT
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        simbolo
    FROM {{ source ('dbfinanceiro', 'tickers') }}
),


renamed AS (
    SELECT
        CAST("Date" AS DATE) AS Data,
        "Open"  AS valor_abertura,
        "High"  AS maior_valor,
        "Low"   AS menor_valor,
        "Close" AS valor_fechamento,
        simbolo
    FROM source
)

SELECT * FROM renamed