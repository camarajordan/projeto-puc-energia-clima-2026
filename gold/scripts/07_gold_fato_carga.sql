-- 07 - Fato_Carga (Claude version)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_fato_carga;

CREATE TABLE db_energia_clima_puc.gold_fato_carga
WITH (
    format               = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/fato_carga/',
    partitioned_by       = ARRAY['year', 'month']
) AS
SELECT
    CAST(id_subsistema          AS VARCHAR) AS id_subsistema,
    nom_subsistema,
    CAST(event_date             AS DATE)    AS event_date,
    CAST(val_cargaenergiamwmed  AS DOUBLE)  AS val_cargaenergiamwmed,
    CAST(year  AS VARCHAR)                  AS year,
    CAST(month AS VARCHAR)                  AS month
FROM db_energia_clima_puc.silver_carga_energia_di
WHERE event_date IS NOT NULL;