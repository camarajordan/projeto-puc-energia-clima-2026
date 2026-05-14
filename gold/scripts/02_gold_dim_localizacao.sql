-- 02 - Dim_Localizacao (Claude version)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_dim_localizacao;

CREATE TABLE db_energia_clima_puc.gold_dim_localizacao
WITH (
    format               = 'PARQUET',
    parquet_compression  = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/dim_localizacao/'
) AS
SELECT DISTINCT
    CAST(e.id_estacao  AS VARCHAR)  AS id_estacao,
    CAST(e.id_municipio AS INTEGER) AS id_municipio,
    e.estacao,
    CAST(e.data_fundacao AS DATE)   AS data_fundacao,
    CAST(e.latitude   AS DOUBLE)    AS latitude,
    CAST(e.longitude  AS DOUBLE)    AS longitude,
    CAST(e.altitude   AS DOUBLE)    AS altitude,
    d.nome                          AS nome_municipio,
    d.sigla_uf,
    d.nome_uf,
    d.nome_regiao
FROM db_energia_clima_puc.silver_estacoes e
LEFT JOIN db_energia_clima_puc.silver_diretorios_brasil d
    ON CAST(e.id_municipio AS INTEGER) = CAST(d.id_municipio AS INTEGER)
WHERE e.id_estacao IS NOT NULL;