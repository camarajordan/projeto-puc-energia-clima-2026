-- 03 - Dim_Usina (Claude version)
DROP TABLE IF EXISTS db_energia_clima_puc.gold_dim_usina;

CREATE TABLE db_energia_clima_puc.gold_dim_usina
WITH (
    format               = 'PARQUET',
    parquet_compression  = 'SNAPPY',
    external_location    = 's3://projeto-puc-energia-clima-2026/gold/dim_usina/'
) AS
WITH capacidade AS (
    SELECT DISTINCT
        id_subsistema,
        nom_subsistema,
        id_estado,
        nom_estado,
        nom_tipousina,
        nom_usina,
        ceg,
        nom_modalidadeoperacao,
        CAST(val_potenciaefetiva AS DOUBLE) AS val_potenciaefetiva,
        dat_entradaoperacao,
        dat_desativacao
    FROM db_energia_clima_puc.silver_capacidade_geracao
),
modalidade AS (
    SELECT DISTINCT
        nom_usina,
        ceg,
        id_ons,
        nom_modalidadeoperacao,
        CAST(val_potenciaautorizada AS DOUBLE) AS val_potenciaautorizada,
        id_estado,
        nom_estado,
        sts_aneel
    FROM db_energia_clima_puc.silver_modalidade_usina
)
SELECT DISTINCT
    capacidade.id_subsistema,
    capacidade.nom_subsistema,
    coalesce(capacidade.id_estado, modalidade.id_estado)                        AS id_estado,
    coalesce(capacidade.nom_estado, modalidade.nom_estado)                      AS nom_estado,
    capacidade.nom_tipousina,
    coalesce(capacidade.nom_usina, modalidade.nom_usina)                        AS nom_usina,
    coalesce(capacidade.ceg, modalidade.ceg)                                    AS ceg,
    modalidade.id_ons,
    coalesce(capacidade.nom_modalidadeoperacao, modalidade.nom_modalidadeoperacao) AS nom_modalidadeoperacao,
    capacidade.val_potenciaefetiva,
    modalidade.val_potenciaautorizada,
    capacidade.dat_entradaoperacao,
    capacidade.dat_desativacao,
    modalidade.sts_aneel
FROM capacidade
LEFT JOIN modalidade
    ON capacidade.ceg = modalidade.ceg;