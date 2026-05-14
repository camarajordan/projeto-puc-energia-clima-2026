-- 06 - Fato_Hidrologia (Claude version)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_fato_hidrologia;

CREATE TABLE db_energia_clima_puc.gold_fato_hidrologia
WITH (
    format               = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/fato_hidrologia/',
    partitioned_by       = ARRAY['year', 'month']
) AS
SELECT
    CAST(id_reservatorio             AS VARCHAR) AS id_reservatorio,
    nom_reservatorio,
    nom_bacia,
    nom_subsistema,
    nom_ree,
    tip_reservatorio,
    CAST(num_ordemcs                 AS DOUBLE)  AS num_ordemcs,
    CAST(cod_usina                   AS DOUBLE)  AS cod_usina,
    CAST(event_date                  AS DATE)    AS event_date,
    CAST(val_nivelmontante           AS DOUBLE)  AS val_nivelmontante,
    CAST(val_niveljusante            AS DOUBLE)  AS val_niveljusante,
    CAST(val_volumeutilcon           AS DOUBLE)  AS val_volumeutilcon,
    CAST(val_vazaoafluente           AS DOUBLE)  AS val_vazaoafluente,
    CAST(val_vazaoturbinada          AS DOUBLE)  AS val_vazaoturbinada,
    CAST(val_vazaovertida            AS DOUBLE)  AS val_vazaovertida,
    CAST(val_vazaooutrasestruturas   AS DOUBLE)  AS val_vazaooutrasestruturas,
    CAST(val_vazaodefluente          AS DOUBLE)  AS val_vazaodefluente,
    CAST(val_vazaotransferida        AS DOUBLE)  AS val_vazaotransferida,
    CAST(val_vazaonatural            AS DOUBLE)  AS val_vazaonatural,
    CAST(val_vazaoartificial         AS DOUBLE)  AS val_vazaoartificial,
    CAST(val_vazaoincremental        AS DOUBLE)  AS val_vazaoincremental,
    CAST(val_vazaoevaporacaoliquida  AS DOUBLE)  AS val_vazaoevaporacaoliquida,
    CAST(val_vazaousoconsuntivo      AS DOUBLE)  AS val_vazaousoconsuntivo,
    CAST(year  AS VARCHAR)                       AS year,
    CAST(month AS VARCHAR)                       AS month
FROM db_energia_clima_puc.silver_dados_hidrologicos_di
WHERE event_date IS NOT NULL;