-- 01 - Dim_Tempo (Claude version)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_dim_tempo;

CREATE TABLE db_energia_clima_puc.gold_dim_tempo
WITH (
    format               = 'PARQUET',
    parquet_compression  = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/dim_tempo/'
) AS
WITH todas_datas AS (
    SELECT event_date FROM db_energia_clima_puc.silver_microdados
    UNION
    SELECT event_date FROM db_energia_clima_puc.silver_geracao_usina_2_ho
    UNION
    SELECT event_date FROM db_energia_clima_puc.silver_carga_energia_di
    UNION
    SELECT event_date FROM db_energia_clima_puc.silver_dados_hidrologicos_di
)
SELECT DISTINCT
    CAST(event_date AS DATE)                                        AS event_date,
    CAST(EXTRACT(YEAR    FROM event_date) AS INTEGER)               AS ano,
    CAST(EXTRACT(QUARTER FROM event_date) AS INTEGER)               AS trimestre,
    CAST(EXTRACT(MONTH   FROM event_date) AS INTEGER)               AS mes,
    date_format(CAST(event_date AS TIMESTAMP), '%M')                AS nome_mes,
    CAST(EXTRACT(DAY     FROM event_date) AS INTEGER)               AS dia,
    CAST(EXTRACT(DAY_OF_WEEK FROM event_date) AS INTEGER)           AS dia_semana_numero,
    date_format(CAST(event_date AS TIMESTAMP), '%W')                AS nome_dia_semana,
    CASE
        WHEN EXTRACT(DAY_OF_WEEK FROM event_date) IN (6, 7) THEN true
        ELSE false
    END                                                             AS fim_de_semana
FROM todas_datas
WHERE event_date IS NOT NULL;