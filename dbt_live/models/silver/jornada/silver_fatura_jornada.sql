WITH fatura_jornada AS (
    SELECT
        n_nota,
        data_de_pregao,
        qted,
        merc,
        txop,
        cotacao
    FROM
        {{ ref('bronze_fatura_jornada') }}
),

fatura_jornada_small AS (
    SELECT
        n_nota,
        data_de_pregao,
        tx_corretagem,
        taxa
    FROM
        {{ ref('bronze_fatura_jornada_small') }}
),

kpi_calculated AS (
    SELECT
        r.n_nota,
        r.data_de_pregao,
        r.qted,
        r.merc,
        r.txop,
        s.tx_corretagem,
        r.cotacao,
        ROUND((r.qted * r.cotacao * (1 - (s.tx_corretagem + r.txop) / 100)), 2) AS movimentacao
    FROM
        fatura_jornada r
    JOIN
        fatura_jornada_small s
    ON
        r.n_nota = s.n_nota
        AND r.data_de_pregao = s.data_de_pregao
)

SELECT * 
FROM kpi_calculated
