WITH source AS (
    SELECT
        "ticker",
        "longName",
        "country",
        "industry",
        "sector",
        "fullTimeEmployees"
    FROM {{ source ('dbfinanceiro', 'info') }}
),


renamed AS (
    SELECT
        "ticker" as simbolo,
        "longName" as nome,
        "country" as pais,
        "industry" as industria,
        "sector" as setor,
        "fullTimeEmployees" as colaboradores
    FROM source
)

SELECT * FROM renamed