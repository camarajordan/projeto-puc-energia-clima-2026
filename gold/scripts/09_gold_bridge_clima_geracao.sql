-- 09 - Bridge_Clima_Geracao (Claude analytic)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_bridge_clima_geracao;

CREATE TABLE db_energia_clima_puc.gold_bridge_clima_geracao
WITH (
    format               = 'PARQUET',
    parquet_compression  = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/bridge_clima_geracao/',
    partitioned_by       = ARRAY['year', 'month']
) AS
WITH

-- De-para: sigla UF → nome do estado como aparece na tabela de geração ONS
de_para_uf_estado AS (
    SELECT DISTINCT
        CAST(id_estado  AS VARCHAR) AS id_estado,
        nom_estado
    FROM db_energia_clima_puc.silver_geracao_usina_2_ho
    WHERE nom_estado IS NOT NULL
),

-- Geração diária por estado, pivotada por tipo de usina
geracao_por_estado AS (
    SELECT
        event_date,
        nom_estado,
        nom_subsistema,
        -- Eólica: correlaciona com vento_velocidade_media
        SUM(CASE WHEN nom_tipousina LIKE '%Eólica%' OR nom_tipousina LIKE '%Eolica%'
                 THEN CAST(val_geracao_total_mwmed AS DOUBLE) END) AS geracao_eolica_mwmed,
        -- Solar/Fotovoltaica: correlaciona com radiacao_global
        SUM(CASE WHEN nom_tipousina LIKE '%Solar%' OR nom_tipousina LIKE '%Fotovolt%'
                 THEN CAST(val_geracao_total_mwmed AS DOUBLE) END) AS geracao_solar_mwmed,
        -- Hidrelétrica: correlaciona com precipitacao_total
        SUM(CASE WHEN nom_tipousina LIKE '%Hidrel%'
                 THEN CAST(val_geracao_total_mwmed AS DOUBLE) END) AS geracao_hidro_mwmed,
        -- Térmica: correlaciona com temperatura (termopico de demanda)
        SUM(CASE WHEN nom_tipousina LIKE '%Térmica%' OR nom_tipousina LIKE '%Termica%'
                 THEN CAST(val_geracao_total_mwmed AS DOUBLE) END) AS geracao_termica_mwmed,
        -- Total geral
        SUM(CAST(val_geracao_total_mwmed AS DOUBLE))                AS geracao_total_mwmed
    FROM db_energia_clima_puc.silver_geracao_usina_2_ho
    WHERE event_date IS NOT NULL
    GROUP BY event_date, nom_estado, nom_subsistema
),

-- Carga diária por subsistema
carga_por_subsistema AS (
    SELECT
        event_date,
        nom_subsistema,
        SUM(CAST(val_cargaenergiamwmed AS DOUBLE)) AS val_cargaenergiamwmed
    FROM db_energia_clima_puc.silver_carga_energia_di
    WHERE event_date IS NOT NULL
    GROUP BY event_date, nom_subsistema
),

-- Clima diário por estação enriquecido com localização
clima_com_localizacao AS (
    SELECT
        m.event_date,
        m.id_estacao,
        loc.sigla_uf,
        loc.nome_uf,
        loc.nome_regiao,
        loc.nome_municipio,
        loc.latitude,
        loc.longitude,
        -- Variáveis climáticas
        CAST(m.precipitacao_total     AS DOUBLE) AS precipitacao_total,
        CAST(m.radiacao_global        AS DOUBLE) AS radiacao_global,
        CAST(m.temperatura_media      AS DOUBLE) AS temperatura_media,
        CAST(m.temperatura_maxima     AS DOUBLE) AS temperatura_maxima,
        CAST(m.temperatura_minima     AS DOUBLE) AS temperatura_minima,
        CAST(m.vento_velocidade_media AS DOUBLE) AS vento_velocidade_media,
        CAST(m.year  AS VARCHAR)                 AS year,
        CAST(m.month AS VARCHAR)                 AS month
    FROM db_energia_clima_puc.silver_microdados m
    INNER JOIN db_energia_clima_puc.gold_dim_localizacao loc
        ON CAST(m.id_estacao AS VARCHAR) = loc.id_estacao
    WHERE m.event_date IS NOT NULL
)

-- Resultado final: clima + geração por tipo + carga, linkados por estado e data
SELECT
    c.event_date,
    c.id_estacao,
    c.sigla_uf,
    c.nome_uf,
    c.nome_regiao,
    c.nome_municipio,
    c.latitude,
    c.longitude,
    -- Variáveis climáticas
    c.precipitacao_total,
    c.radiacao_global,
    c.temperatura_media,
    c.temperatura_maxima,
    c.temperatura_minima,
    c.vento_velocidade_media,
    -- Geração por tipo de fonte (pivotada)
    g.nom_subsistema,
    g.geracao_eolica_mwmed,    -- para correlacionar com vento
    g.geracao_solar_mwmed,     -- para correlacionar com radiação solar
    g.geracao_hidro_mwmed,     -- para correlacionar com precipitação
    g.geracao_termica_mwmed,   -- para correlacionar com temperatura
    g.geracao_total_mwmed,
    -- Demanda elétrica do subsistema
    ca.val_cargaenergiamwmed,
    c.year,
    c.month
FROM clima_com_localizacao c
-- Join clima → geração via nome do estado
LEFT JOIN de_para_uf_estado dp
    ON c.nome_uf = dp.nom_estado
LEFT JOIN geracao_por_estado g
    ON  c.event_date   = g.event_date
    AND dp.nom_estado  = g.nom_estado
-- Join geração → carga via subsistema
LEFT JOIN carga_por_subsistema ca
    ON  c.event_date      = ca.event_date
    AND g.nom_subsistema  = ca.nom_subsistema;