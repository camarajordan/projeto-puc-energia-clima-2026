-- 08 - Fato_Eficiencia_Hidro (Claude analytic)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_fato_eficiencia_hidro;

CREATE TABLE db_energia_clima_puc.gold_fato_eficiencia_hidro
WITH (
    format               = 'PARQUET',
    parquet_compression  = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/fato_eficiencia_hidro/',
    partitioned_by       = ARRAY['year', 'month']
) AS
WITH geracao_hidro AS (
    -- Agrega geração hidrelétrica por subsistema e dia
    SELECT
        event_date,
        nom_subsistema,
        SUM(CAST(val_geracao_total_mwmed AS DOUBLE)) AS val_geracao_hidro_mwmed,
        year,
        month
    FROM db_energia_clima_puc.silver_geracao_usina_2_ho
    WHERE nom_tipousina LIKE '%Hidrel%'
      AND event_date IS NOT NULL
    GROUP BY event_date, nom_subsistema, year, month
),
hidrologia_agregada AS (
    -- Agrega métricas de reservatório por subsistema e dia
    SELECT
        event_date,
        nom_subsistema,
        SUM(CAST(val_vazaoturbinada AS DOUBLE)) AS val_vazaoturbinada_total,
        SUM(CAST(val_vazaoafluente  AS DOUBLE)) AS val_vazaoafluente_total,
        SUM(CAST(val_vazaodefluente AS DOUBLE)) AS val_vazaodefluente_total,
        AVG(CAST(val_volumeutilcon  AS DOUBLE)) AS val_volumeutilcon_medio,
        year,
        month
    FROM db_energia_clima_puc.silver_dados_hidrologicos_di
    WHERE event_date IS NOT NULL
    GROUP BY event_date, nom_subsistema, year, month
)
SELECT
    h.event_date,
    h.nom_subsistema,
    h.val_vazaoturbinada_total,
    h.val_vazaoafluente_total,
    h.val_vazaodefluente_total,
    h.val_volumeutilcon_medio,
    g.val_geracao_hidro_mwmed,
    -- Indicador: produtividade hídrica (MWmed por m³/s turbinado)
    CASE
        WHEN h.val_vazaoturbinada_total > 0
        THEN g.val_geracao_hidro_mwmed / h.val_vazaoturbinada_total
        ELSE NULL
    END AS eficiencia_mwmed_por_m3s,
    h.year,
    h.month
FROM hidrologia_agregada h
LEFT JOIN geracao_hidro g
    ON  h.event_date     = g.event_date
    AND h.nom_subsistema = g.nom_subsistema;
