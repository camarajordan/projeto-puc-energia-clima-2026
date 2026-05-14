-- 04 - Fato_Clima (Claude version)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_fato_clima;

CREATE TABLE db_energia_clima_puc.gold_fato_clima
WITH (
    format               = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/fato_clima/',
    partitioned_by       = ARRAY['year', 'month']
) AS
SELECT
    CAST(id_estacao             AS VARCHAR) AS id_estacao,
    CAST(event_date             AS DATE)    AS event_date,
    CAST(precipitacao_total     AS DOUBLE)  AS precipitacao_total,
    CAST(radiacao_global        AS DOUBLE)  AS radiacao_global,
    CAST(temperatura_media      AS DOUBLE)  AS temperatura_media,
    CAST(temperatura_maxima     AS DOUBLE)  AS temperatura_maxima,
    CAST(temperatura_minima     AS DOUBLE)  AS temperatura_minima,
    CAST(vento_velocidade_media AS DOUBLE)  AS vento_velocidade_media,
    timezone,
    CAST(year  AS VARCHAR)                  AS year,
    CAST(month AS VARCHAR)                  AS month
FROM db_energia_clima_puc.silver_microdados
WHERE event_date IS NOT NULL;