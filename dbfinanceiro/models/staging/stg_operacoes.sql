WITH source AS (
    SELECT
        "Date",
        simbolo,
        "actions",
        "quantity"
    FROM {{ source ('dbfinanceiro', 'operacoes') }}
),

renamed AS (
    SELECT
        CAST("Date" AS DATE) AS Data,
        simbolo,
        "actions" AS tipo_movimento,
        "quantity" As quantidade
    FROM source
)

SELECT * FROM renamed