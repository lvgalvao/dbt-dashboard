WITH fatura_redrex AS (
    SELECT
        n_nota,
        data_de_pregao,
        qted,
        merc,
        txop,
        cotacao
    FROM
        {{ ref('bronze_fatura_redrex') }}
),

fatura_redrex_small AS (
    SELECT
        n_nota,
        data_de_pregao,
        tx_corretagem
    FROM
        {{ ref('bronze_fatura_redrex_small') }}
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
        (r.qted * r.cotacao * (1 - (s.tx_corretagem + r.txop) / 100)) AS movimentacao
    FROM
        fatura_redrex r
    JOIN
        fatura_redrex_small s
    ON
        r.n_nota = s.n_nota
        AND r.data_de_pregao = s.data_de_pregao
)

SELECT * 
FROM kpi_calculated