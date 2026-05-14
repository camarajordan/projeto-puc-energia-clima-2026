"""
Script AWS Glue — Camada Silver
Transforma os dados brutos da Bronze em dados limpos, tipados e particionados.

Datasets ONS:
  - dados_hidrologicos_di  → silver/ons/dados_hidrologicos_di/year=/month=/
  - carga_energia_di       → silver/ons/carga_energia_di/year=/month=/
  - geracao_usina_2_ho     → silver/ons/geracao_usina_2_ho/year=/month=/  (agregado diário)
  - capacidade_geracao     → silver/ons/capacidade_geracao/                (dimensão estática)
  - modalidade_usina       → silver/ons/modalidade_usina/                  (dimensão estática)

Datasets INMET:
  - microdados             → silver/inmet/microdados/year=/month=/         (agregado diário)
  - estacoes               → silver/inmet/estacoes/                        (dimensão estática)

Dimensões geográficas:
  - municipios             → silver/diretorios_brasil/municipios/           (dimensão estática)
    ↳ elo entre estacoes (id_municipio) e ONS (sigla_uf / nom_estado)

Nota sobre joins geográficos:
  estacoes.id_municipio → municipios.id_municipio → municipios.sigla_uf → ONS por estado
"""

import re
import sys
from collections import defaultdict
from typing import Dict, List, Set
from urllib.parse import urlparse

import boto3

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# ---------------------------------------------------------------------------
# Caminhos S3
# ---------------------------------------------------------------------------
BUCKET_BASE = "s3://projeto-puc-energia-clima-2026"
BUCKET_NAME  = "projeto-puc-energia-clima-2026"
S3_CLIENT    = boto3.client("s3")

# Bronze — fontes
SRC_ONS_HIDRO   = f"{BUCKET_BASE}/bronze/ons/dados_hidrologicos_di/"
SRC_ONS_CARGA   = f"{BUCKET_BASE}/bronze/ons/carga_energia_di/"
SRC_ONS_GERACAO = f"{BUCKET_BASE}/bronze/ons/geracao_usina_2_ho/"
SRC_ONS_CAP     = f"{BUCKET_BASE}/bronze/ons/capacidade-geracao/capacidade_geracao.parquet"
SRC_ONS_MOD     = f"{BUCKET_BASE}/bronze/ons/modalidade_usina/modalidade_usina.parquet"
SRC_INMET_MICRO = f"{BUCKET_BASE}/bronze/inmet/microdados/"
SRC_INMET_EST   = f"{BUCKET_BASE}/bronze/inmet/estacoes/"
SRC_DIR_MUN     = f"{BUCKET_BASE}/bronze/diretorios_brasil/municipios/diretorios_municipios_raw.csv"

# Silver — destinos
DEST_HIDRO   = f"{BUCKET_BASE}/silver/ons/dados_hidrologicos_di/"
DEST_CARGA   = f"{BUCKET_BASE}/silver/ons/carga_energia_di/"
DEST_GERACAO = f"{BUCKET_BASE}/silver/ons/geracao_usina_2_ho/"
DEST_CAP     = f"{BUCKET_BASE}/silver/ons/capacidade_geracao/"
DEST_MOD     = f"{BUCKET_BASE}/silver/ons/modalidade_usina/"
DEST_INMET   = f"{BUCKET_BASE}/silver/inmet/microdados/"
DEST_EST     = f"{BUCKET_BASE}/silver/inmet/estacoes/"
DEST_DIR_MUN = f"{BUCKET_BASE}/silver/diretorios_brasil/municipios/"

TIMEZONE = "America/Sao_Paulo"

# ---------------------------------------------------------------------------
# Colunas por dataset
# (verificadas contra os schemas reais dos arquivos Parquet/CSV da Bronze)
# ---------------------------------------------------------------------------

COLS_HIDRO = [
    # Dimensões
    "id_reservatorio", "nom_reservatorio", "nom_bacia", "nom_subsistema", "nom_ree",
    "tip_reservatorio", "num_ordemcs", "cod_usina",
    # Temporal
    "din_instante",
    # Nível (metros)
    "val_nivelmontante", "val_niveljusante",
    # Volume e vazões — cobertura completa do dicionário ONS
    "val_volumeutilcon",
    "val_vazaoafluente", "val_vazaoturbinada", "val_vazaovertida",
    "val_vazaooutrasestruturas", "val_vazaodefluente", "val_vazaotransferida",
    "val_vazaonatural", "val_vazaoartificial", "val_vazaoincremental",
    "val_vazaoevaporacaoliquida", "val_vazaousoconsuntivo",
]
# num_ordemcs e cod_usina já chegam como int32 nativo — não precisam de cast
CAST_HIDRO_DOUBLE = [
    "val_nivelmontante", "val_niveljusante", "val_volumeutilcon",
    "val_vazaoafluente", "val_vazaoturbinada", "val_vazaovertida",
    "val_vazaooutrasestruturas", "val_vazaodefluente", "val_vazaotransferida",
    "val_vazaonatural", "val_vazaoartificial", "val_vazaoincremental",
    "val_vazaoevaporacaoliquida", "val_vazaousoconsuntivo",
]

COLS_CARGA = [
    "id_subsistema", "nom_subsistema",
    "din_instante",
    "val_cargaenergiamwmed",
]

COLS_GERACAO = [
    "din_instante",
    "id_subsistema", "nom_subsistema",
    "id_estado", "nom_estado",
    "nom_tipousina",
    "nom_usina", "id_ons",
    "val_geracao",
]

COLS_CAP = [
    "id_subsistema", "nom_subsistema",
    "id_estado", "nom_estado",
    "nom_tipousina", "nom_usina", "ceg", "id_ons",
    "nom_modalidadeoperacao",
    "val_potenciaefetiva",
    "dat_entradaoperacao", "dat_desativacao",
]

COLS_MOD = [
    "nom_usina", "ceg", "id_ons",
    "nom_modalidadeoperacao",
    "val_potenciaautorizada",
    "id_estado", "nom_estado",
    "sts_aneel",
]

COLS_INMET = [
    # data e hora necessários para construção do timestamp antes do select
    "id_estacao", "data", "hora",
    "precipitacao_total", "radiacao_global",
    "temperatura_bulbo_hora", "vento_velocidade",
]

# Colunas verificadas contra o CSV real: 
# ['id_municipio', 'id_estacao', 'estacao', 'data_fundacao', 'latitude', 'longitude', 'altitude']
# Nota: 'sigla_uf' NÃO existe neste arquivo — vem via join com municipios
COLS_EST = [
    "id_estacao", "id_municipio", "estacao",
    "data_fundacao", "latitude", "longitude", "altitude",
]

COLS_MUN = [
    "id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao",
]

# ---------------------------------------------------------------------------
# Utilitários S3
# ---------------------------------------------------------------------------
def parse_s3_uri(s3_uri: str):
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")


def silver_years_exist(silver_uri: str) -> Set[int]:
    """Retorna os anos já particionados na Silver (prefixos year=XXXX)."""
    _, prefix = parse_s3_uri(silver_uri)
    if not prefix.endswith("/"):
        prefix += "/"
    resp = S3_CLIENT.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter="/")
    years: Set[int] = set()
    for cp in resp.get("CommonPrefixes", []):
        part = cp["Prefix"].rstrip("/").split("/")[-1]
        if part.startswith("year="):
            try:
                years.add(int(part.split("=")[1]))
            except ValueError:
                pass
    return years


def bronze_files_by_year(source_uri: str) -> Dict[int, List[str]]:
    """
    Varre o prefixo Bronze e agrupa URIs de Parquet por ano.
    Suporta arquivos anuais (*_YYYY.parquet) e mensais (*_YYYY_MM.parquet).
    """
    bucket, prefix = parse_s3_uri(source_uri)
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    result: Dict[int, List[str]] = defaultdict(list)

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".parquet"):
                continue
            nome = key.split("/")[-1]
            match = re.search(r"(\d{4})", nome)
            if match:
                year = int(match.group(1))
                result[year].append(f"s3://{bucket}/{key}")

    return dict(result)


def arquivo_existe(prefix_s3: str) -> bool:
    resp = S3_CLIENT.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix_s3, MaxKeys=1)
    return "Contents" in resp


# ---------------------------------------------------------------------------
# Utilitários Spark
# ---------------------------------------------------------------------------
def resolve_args() -> dict:
    return getResolvedOptions(sys.argv, ["JOB_NAME"])


def spark_session():
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    # Evita erros de rebase em timestamps de Parquets legados
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    return glue_ctx, spark


def sanitize_parquet_types(df: DataFrame) -> DataFrame:
    """Converte colunas TIME para String antes de gravar — evita erro de tipo não suportado."""
    for field in df.schema.fields:
        if field.dataType.typeName().lower().startswith("time"):
            df = df.withColumn(field.name, F.col(field.name).cast(StringType()))
    return df


def select_existing(df: DataFrame, cols: List[str]) -> DataFrame:
    """Seleciona apenas colunas que existem no DataFrame — seguro contra schemas variáveis."""
    return df.select([c for c in cols if c in df.columns])


def cast_to_double(df: DataFrame, cols: List[str]) -> DataFrame:
    """Cast seguro de string (com possível vírgula decimal) para Double."""
    for col in cols:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.regexp_replace(F.col(col).cast(StringType()), ",", ".").cast(DoubleType())
            )
    return df


def drop_all_null(df: DataFrame) -> DataFrame:
    return df.na.drop(how="all")


def add_year_month(df: DataFrame, date_col: str) -> DataFrame:
    return (
        df.withColumn("year",  F.year(F.col(date_col)).cast(IntegerType()))
          .withColumn("month", F.month(F.col(date_col)).cast(IntegerType()))
    )


def write_partitioned(df: DataFrame, dest: str) -> None:
    df = sanitize_parquet_types(df)
    (
        df.repartition("year", "month")
          .write.mode("overwrite")
          .partitionBy("year", "month")
          .parquet(dest)
    )


def write_single(df: DataFrame, dest: str) -> None:
    df = sanitize_parquet_types(df)
    df.coalesce(1).write.mode("overwrite").parquet(dest)


# ---------------------------------------------------------------------------
# Processadores por dataset
# ---------------------------------------------------------------------------
def process_hidrologico(spark: SparkSession) -> None:
    print("\n--- Silver: dados_hidrologicos_di ---")
    bronze_anos = bronze_files_by_year(SRC_ONS_HIDRO)
    silver_anos = silver_years_exist(DEST_HIDRO)
    pendentes   = sorted(set(bronze_anos) - silver_anos)
    if not pendentes:
        print("  Já atualizado. Nada a fazer.")
        return
    for ano in pendentes:
        print(f"  Processando {ano}...")
        df = spark.read.parquet(*bronze_anos[ano])
        df = select_existing(df, COLS_HIDRO)
        df = cast_to_double(df, CAST_HIDRO_DOUBLE)
        df = df.withColumn("event_date", F.to_date(F.col("din_instante"))).drop("din_instante")
        df = drop_all_null(df)
        df = add_year_month(df, "event_date")
        write_partitioned(df, DEST_HIDRO)
        print(f"  {ano} gravado.")


def process_carga(spark: SparkSession) -> None:
    print("\n--- Silver: carga_energia_di ---")
    bronze_anos = bronze_files_by_year(SRC_ONS_CARGA)
    silver_anos = silver_years_exist(DEST_CARGA)
    pendentes   = sorted(set(bronze_anos) - silver_anos)
    if not pendentes:
        print("  Já atualizado. Nada a fazer.")
        return
    for ano in pendentes:
        print(f"  Processando {ano}...")
        df = spark.read.parquet(*bronze_anos[ano])
        df = select_existing(df, COLS_CARGA)
        df = cast_to_double(df, ["val_cargaenergiamwmed"])
        df = df.withColumn("event_date", F.to_date(F.col("din_instante"))).drop("din_instante")
        df = drop_all_null(df)
        df = add_year_month(df, "event_date")
        write_partitioned(df, DEST_CARGA)
        print(f"  {ano} gravado.")


def process_geracao(spark: SparkSession) -> None:
    """Lê dados horários e agrega para diário por subsistema/estado/tipo de usina."""
    print("\n--- Silver: geracao_usina_2_ho (agregação diária) ---")
    bronze_anos = bronze_files_by_year(SRC_ONS_GERACAO)
    silver_anos = silver_years_exist(DEST_GERACAO)
    pendentes   = sorted(set(bronze_anos) - silver_anos)
    if not pendentes:
        print("  Já atualizado. Nada a fazer.")
        return
    for ano in pendentes:
        print(f"  Processando {ano} ({len(bronze_anos[ano])} arquivo(s))...")
        df = spark.read.parquet(*bronze_anos[ano])
        df = select_existing(df, COLS_GERACAO)
        df = cast_to_double(df, ["val_geracao"])
        df = df.withColumn("event_date", F.to_date(F.col("din_instante"))).drop("din_instante")
        df = (
            df.groupBy(
                "event_date",
                "id_subsistema", "nom_subsistema",
                "id_estado", "nom_estado",
                "nom_tipousina",
            )
            .agg(
                F.sum("val_geracao").alias("val_geracao_total_mwmed"),
                F.countDistinct("nom_usina").alias("qtd_usinas_ativas"),
            )
        )
        df = drop_all_null(df)
        df = add_year_month(df, "event_date")
        write_partitioned(df, DEST_GERACAO)
        print(f"  {ano} gravado.")


def process_capacidade(spark: SparkSession) -> None:
    print("\n--- Silver: capacidade_geracao (dimensão) ---")
    if arquivo_existe("silver/ons/capacidade_geracao/"):
        print("  Já existe. Pulando.")
        return
    df = spark.read.parquet(SRC_ONS_CAP)
    df = select_existing(df, COLS_CAP)
    df = cast_to_double(df, ["val_potenciaefetiva"])
    df = drop_all_null(df)
    write_single(df, DEST_CAP)
    print("  Gravado.")


def process_modalidade(spark: SparkSession) -> None:
    print("\n--- Silver: modalidade_usina (dimensão) ---")
    if arquivo_existe("silver/ons/modalidade_usina/"):
        print("  Já existe. Pulando.")
        return
    df = spark.read.parquet(SRC_ONS_MOD)
    df = select_existing(df, COLS_MOD)
    df = cast_to_double(df, ["val_potenciaautorizada"])
    df = drop_all_null(df)
    write_single(df, DEST_MOD)
    print("  Gravado.")


def process_inmet_microdados(spark: SparkSession) -> None:
    print("\n--- Silver: INMET microdados (agregação diária) ---")
    bronze_anos = bronze_files_by_year(SRC_INMET_MICRO)
    silver_anos = silver_years_exist(DEST_INMET)
    pendentes   = sorted(set(bronze_anos) - silver_anos)
    if not pendentes:
        print("  Já atualizado. Nada a fazer.")
        return
    for ano in pendentes:
        print(f"  Processando {ano}...")
        df = spark.read.parquet(*bronze_anos[ano])
        df = select_existing(df, COLS_INMET)
        df = cast_to_double(df, [
            "precipitacao_total", "radiacao_global",
            "temperatura_bulbo_hora", "vento_velocidade",
        ])

        # Constrói timestamp local a partir das colunas data + hora (string UTC)
        data_col = F.to_date(F.col("data").cast("string"))
        hora_str = F.regexp_replace(F.col("hora").cast("string"), r"\s*UTC\s*", "")
        hora_ts  = F.to_timestamp(
            F.concat_ws(" ", F.date_format(data_col, "yyyy-MM-dd"), hora_str),
            "yyyy-MM-dd HH:mm:ss",
        )

        df = (
            df.withColumn("event_timestamp_utc", hora_ts)
              .withColumn("event_timestamp_local", F.from_utc_timestamp("event_timestamp_utc", TIMEZONE))
              .withColumn("event_date", F.to_date("event_timestamp_local"))
        )

        # Agrega horário → diário por estação
        df = (
            df.filter(F.col("event_timestamp_local").isNotNull())
              .groupBy("id_estacao", "event_date")
              .agg(
                  F.sum("precipitacao_total").alias("precipitacao_total"),
                  F.avg("radiacao_global").alias("radiacao_global"),
                  F.avg("temperatura_bulbo_hora").alias("temperatura_media"),
                  F.max("temperatura_bulbo_hora").alias("temperatura_maxima"),
                  F.min("temperatura_bulbo_hora").alias("temperatura_minima"),
                  F.avg("vento_velocidade").alias("vento_velocidade_media"),
              )
              .withColumn("timezone", F.lit(TIMEZONE))
        )

        df = drop_all_null(df)
        df = add_year_month(df, "event_date")
        write_partitioned(df, DEST_INMET)
        print(f"  {ano} gravado.")


def process_inmet_estacoes(spark: SparkSession) -> None:
    """
    Dimensão de estações.
    Colunas verificadas contra o CSV real:
    ['id_municipio', 'id_estacao', 'estacao', 'data_fundacao', 'latitude', 'longitude', 'altitude']
    Nota: sigla_uf NÃO existe aqui — vem via join com municipios usando id_municipio.
    """
    print("\n--- Silver: INMET estações (dimensão) ---")
    if arquivo_existe("silver/inmet/estacoes/"):
        print("  Já existe. Pulando.")
        return
    df = spark.read.option("header", True).option("inferSchema", True).csv(SRC_INMET_EST)
    df = select_existing(df, COLS_EST)
    df = cast_to_double(df, ["latitude", "longitude", "altitude"])
    df = drop_all_null(df)
    write_single(df, DEST_EST)
    print("  Gravado.")


def process_municipios(spark: SparkSession) -> None:
    """
    Dimensão geográfica — elo entre estações INMET e dados ONS por estado.
    Join: estacoes.id_municipio → municipios.id_municipio → municipios.sigla_uf
    """
    print("\n--- Silver: Diretórios Municípios (dimensão) ---")
    if arquivo_existe("silver/diretorios_brasil/municipios/"):
        print("  Já existe. Pulando.")
        return
    df = spark.read.option("header", True).option("inferSchema", True).csv(SRC_DIR_MUN)
    df = select_existing(df, COLS_MUN)
    df = drop_all_null(df)
    write_single(df, DEST_DIR_MUN)
    print("  Gravado.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = resolve_args()
    glue_ctx, spark = spark_session()
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    # ONS — séries temporais
    process_hidrologico(spark)
    process_carga(spark)
    process_geracao(spark)

    # ONS — dimensões
    process_capacidade(spark)
    process_modalidade(spark)

    # INMET
    process_inmet_microdados(spark)
    process_inmet_estacoes(spark)

    # Geográfico
    process_municipios(spark)

    job.commit()
    print("\n>>> SILVER FINALIZADO COM SUCESSO <<<")


if __name__ == "__main__":
    main()
