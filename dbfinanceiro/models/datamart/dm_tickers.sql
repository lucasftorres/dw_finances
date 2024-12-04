WITH tickers AS (
    SELECT
        data,
        simbolo,
        valor_abertura,
        menor_valor,
        maior_valor,
        valor_fechamento
    FROM {{ ref ('stg_tickers') }}
),

operacoes AS (
    SELECT
        data,
        simbolo,
        tipo_movimento,
        quantidade
    FROM {{ ref('stg_operacoes') }}
),

joined AS (
    SELECT
        t.data,
        t.simbolo,
        t.valor_abertura,
        t.menor_valor,
        t.maior_valor,
        t.valor_fechamento,
        o.quantidade,
        (o.quantidade * t.valor_fechamento) AS valor,
        CASE
            WHEN o.tipo_movimento = 'sell' THEN (o.quantidade * (t.valor_fechamento - t.valor_abertura))
            ELSE -(o.quantidade * t.valor_fechamento)
        END AS ganho
    FROM 
        tickers AS t
        INNER JOIN
        operacoes AS o
        ON t.data = o.data AND t.simbolo = o.simbolo
),

lastday AS (
    SELECT
        MAX(data) as max_date
    FROM joined
),

filtered AS (
    SELECT
        *
    FROM joined
    WHERE data = (SELECT max_date FROM lastday)
)

SELECT
    data,
    simbolo,
    valor_abertura,
    maior_valor,
    menor_valor,
    quantidade,
    valor,
    ganho
FROM filtered
