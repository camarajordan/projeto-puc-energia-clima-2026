-- 05 - Fato_Geracao (Claude version)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_fato_geracao;

CREATE TABLE db_energia_clima_puc.gold_fato_geracao
WITH (
    format               = 'PARQUET',
    parquet_compression  = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/fato_geracao/',
    partitioned_by       = ARRAY['year', 'month']
) AS
SELECT
    CAST(event_date              AS DATE)    AS event_date,
    CAST(id_subsistema           AS VARCHAR) AS id_subsistema,
    nom_subsistema,
    CAST(id_estado               AS VARCHAR) AS id_estado,
    nom_estado,
    nom_tipousina,
    CAST(val_geracao_total_mwmed AS DOUBLE)  AS val_geracao_total_mwmed,
    CAST(qtd_usinas_ativas       AS BIGINT)  AS qtd_usinas_ativas,
    CAST(year  AS VARCHAR)                   AS year,
    CAST(month AS VARCHAR)                   AS month
FROM db_energia_clima_puc.silver_geracao_usina_2_ho
WHERE event_date IS NOT NULL;