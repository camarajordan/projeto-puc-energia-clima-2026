"""
Script de ingestão - Camada Bronze

Responsabilidades:
- Espelhar arquivos Parquet públicos do ONS para o bucket S3 do projeto
- Extrair e armazenar microdados do INMET via BigQuery (conversão para Parquet em memória)
- Baixar tabelas de dimensão e dicionários de metadados para a pasta `metadata/`

Observações operacionais:
- Os caminhos S3 seguem convenções lowercase para compatibilidade com catalogadores e ferramentas
- A lista `ONS_PREFIXOS_BRONZE` define explicitamente quais prefixos do bucket público ONS serão espelhados
- O fluxo foi projetado para ser idempotente e seguro para reexecução
"""

import os
import re
import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
import gc
import logging
import requests
import gzip
import json
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Autenticação e configurações
# - As credenciais do GCP são fornecidas via variável de ambiente
# - O cliente S3 (`boto3`) opera sobre o bucket de destino definido abaixo
# ---------------------------------------------------------------------------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "myb2bapp-405901-fee9b119d4d9.json"

s3 = boto3.client('s3')
BUCKET_DESTINO = 'projeto-puc-energia-clima-2026'

# S3 - caminhos e convenções
# Observação: usamos lowercase consistente para facilitar integração com crawlers e query engines
PASTA_ONS = 'bronze/ons/'
PASTA_INMET_MICRO = 'bronze/inmet/microdados/'
PASTA_INMET_EST = 'bronze/inmet/estacoes/'
PASTA_DIR_MUN = 'bronze/diretorios_brasil/municipios/'

# Diretórios de metadados (dicionários e documentos de suporte)
PASTA_META_ONS = 'metadata/dicionarios/ons/dados_hidrologicos_di/'
PASTA_META_INMET_MICRO = 'metadata/dicionarios/inmet/microdados/'
PASTA_META_INMET_EST = 'metadata/dicionarios/inmet/estacoes/'
PASTA_META_DIR_MUN = 'metadata/dicionarios/diretorios_municipios/'
PASTA_META_ONS_ROOT = 'metadata/dicionarios/ons/'

ONS_PREFIXOS_BRONZE = [
    'dataset/dados_hidrologicos_di/',
    'dataset/carga_energia_di/',
    'dataset/geracao_usina_2_ho/',
    'dataset/capacidade-geracao/',
    'dataset/modalidade_usina/',
]

# Filtra apenas arquivos que tenham anos dentro do intervalo desejado (inclusive)
ALLOWED_YEARS = set(range(2016, 2025))

# ==========================================
# FUNÇÃO AUXILIAR: IDEMPOTÊNCIA
# ==========================================
def arquivo_ja_existe(caminho_s3):
    response = s3.list_objects_v2(Bucket=BUCKET_DESTINO, Prefix=caminho_s3)
    return 'Contents' in response


def copiar_prefixo_parquet_ons(bucket_origem, prefix_origem, prefix_destino_base):
    """Espelha arquivos Parquet de um prefixo público ONS para o bucket Bronze.

    Regras importantes:
    - Apenas arquivos com extensão `.parquet` são copiados.
    - Se o arquivo contiver um ano no nome, será aplicado o filtro `ALLOWED_YEARS`.
    - Este método usa o paginador S3 para suportar prefixos com >1000 objetos.
    - O destino preserva uma hierarquia por dataset: `<prefix_destino_base>/<dataset>/<arquivo>`.
    """
    paginator = s3.get_paginator('list_objects_v2')
    copiados = 0

    for page in paginator.paginate(Bucket=bucket_origem, Prefix=prefix_origem):
        for obj in page.get('Contents', []):
            chave_origem = obj['Key']
            if not chave_origem.lower().endswith('.parquet'):
                continue

            sufixo = chave_origem[len(prefix_origem):].lstrip('/')
            # Se o nome do arquivo contém um ano, filtra pelo intervalo permitido
            nome_base = os.path.basename(chave_origem)
            ano_match = re.search(r"(\d{4})", nome_base)
            if ano_match:
                try:
                    ano_int = int(ano_match.group(1))
                except ValueError:
                    ano_int = None
                if ano_int is not None and ano_int not in ALLOWED_YEARS:
                    logger.info(f" -> ONS {prefix_origem}: Ignorando {nome_base} (ano {ano_int} fora do intervalo).")
                    continue
            chave_destino = f"{prefix_destino_base}{prefix_origem.rstrip('/').split('/')[-1]}/{sufixo.lower()}"

            if arquivo_ja_existe(chave_destino):
                logger.info(f" -> ONS {prefix_origem}: {os.path.basename(chave_destino)} já existe. Pulando.")
                continue

            s3.copy_object(
                CopySource={'Bucket': bucket_origem, 'Key': chave_origem},
                Bucket=BUCKET_DESTINO,
                Key=chave_destino,
            )
            copiados += 1
            logger.info(f" -> ONS {prefix_origem}: Copiado {os.path.basename(chave_destino)}")

    return copiados

# ==========================================
# PIPELINE PRINCIPAL
# ==========================================
def executar_ingestao_full_bronze():
    # erros_pipeline é local para evitar que múltiplas execuções na mesma sessão acumulem erros
    erros_pipeline = []
    bq_client = bigquery.Client()
    
    # ---------------------------------------------------------
    # FASE 1: ONS (espelho dos Parquets nativos na Bronze)
    # ---------------------------------------------------------
    logger.info("--- FASE 1: Ingestão ONS (Parquet Nativo) ---")
    bucket_origem_ons = 'ons-aws-prod-opendata'
    for prefixo in ONS_PREFIXOS_BRONZE:
        try:
            copiados = copiar_prefixo_parquet_ons(bucket_origem_ons, prefixo, PASTA_ONS)
            logger.info(f" -> ONS {prefixo}: {copiados} arquivo(s) Parquet copiados para a Bronze.")
        except Exception as e:
            erro_msg = f"Fase 1 (ONS Bronze {prefixo}): {e}"
            logger.error(erro_msg)
            erros_pipeline.append(erro_msg)

    # ---------------------------------------------------------
    # FASE 2: INMET MICRODADOS (GCP -> RAM -> S3)
    # ---------------------------------------------------------
    logger.info("\n--- FASE 2: Ingestão INMET Microdados ---")
    for ano in range(2016, 2025):
        nome_arq = f"inmet_microdados_raw_{ano}.parquet"
        chave_destino = f"{PASTA_INMET_MICRO}{nome_arq}"
        
        if arquivo_ja_existe(chave_destino):
            logger.info(f" -> INMET {ano}: Já existe. Pulando.")
            continue
            
        query = f"""
            SELECT * REPLACE (CAST(hora AS STRING) AS hora)
            FROM `basedosdados.br_inmet_bdmep.microdados`
            WHERE ano = {ano}
        """
        try:
            logger.info(f" -> INMET {ano}: Puxando do BigQuery...")
            arrow_table = bq_client.query(query).to_arrow()
            buffer = io.BytesIO()
            pq.write_table(arrow_table, buffer)
            del arrow_table
            gc.collect() 
            
            buffer.seek(0)
            s3.upload_fileobj(buffer, BUCKET_DESTINO, chave_destino)
            del buffer
            gc.collect()
            logger.info(f" -> INMET {ano}: Sucesso!")
        except Exception as e:
            erro_msg = f"Fase 2 (INMET Micro {ano}): {e}"
            logger.error(erro_msg)
            erros_pipeline.append(erro_msg)
            gc.collect()

    # ---------------------------------------------------------
    # FASE 3: INMET ESTAÇÕES (API -> RAM -> S3)
    # ---------------------------------------------------------
    logger.info("\n--- FASE 3: Ingestão INMET Estações ---")
    url_estacoes = "https://basedosdados.org/api/tables/downloadTable?p=YnJfaW5tZXRfYmRtZXA=&q=ZXN0YWNhbw==&d=dHJ1ZQ==&s=ZnJlZQ=="
    chave_destino_est = f"{PASTA_INMET_EST}inmet_estacoes_raw.csv"
    
    if arquivo_ja_existe(chave_destino_est):
        logger.info(" -> INMET Estações: Já existe. Pulando.")
    else:
        try:
            resp = requests.get(url_estacoes, timeout=120)
            csv_puro = gzip.decompress(resp.content)
            s3.put_object(Bucket=BUCKET_DESTINO, Key=chave_destino_est, Body=csv_puro)
            logger.info(" -> INMET Estações: Sucesso!")
        except Exception as e:
            erro_msg = f"Fase 3 (INMET Estações): {e}"
            logger.error(erro_msg)
            erros_pipeline.append(erro_msg)

    # ---------------------------------------------------------
    # FASE 4: DIRETÓRIOS BRASIL (Municípios) via BigQuery
    # ---------------------------------------------------------
    logger.info("\n--- FASE 4: Ingestão Diretórios Brasil (Municípios) ---")
    nome_arq_mun = "diretorios_municipios_raw.csv"
    chave_destino_mun = f"{PASTA_DIR_MUN}{nome_arq_mun}"
    
    if arquivo_ja_existe(chave_destino_mun):
        logger.info(" -> Diretórios Municípios: Já existe. Pulando.")
    else:
        query_mun = "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.municipio`"
        try:
            logger.info(" -> Puxando tabela de municípios do BigQuery...")
            df_mun = bq_client.query(query_mun).to_dataframe()
            
            if not df_mun.empty:
                buffer_csv = io.StringIO()
                df_mun.to_csv(buffer_csv, index=False, encoding='utf-8')
                
                s3.put_object(Bucket=BUCKET_DESTINO, Key=chave_destino_mun, Body=buffer_csv.getvalue().encode('utf-8'))
                logger.info(" -> Diretórios Municípios: Sucesso!")
        except Exception as e:
            erro_msg = f"Fase 4 (Diretórios Municípios): {e}"
            logger.error(erro_msg)
            erros_pipeline.append(erro_msg)

    # ---------------------------------------------------------
    # FASE 5: METADADOS E DICIONÁRIOS (Governança)
    # ---------------------------------------------------------
    logger.info("\n--- FASE 5: Extração de Dicionários de Dados ---")
    
    # 5.1 ONS (Cópia direta)
    for arq in["DicionarioDados_DadosHidrologicosDiarios.json", "DicionarioDados_DadosHidrologicosDiarios.pdf"]:
        chave_dest_meta = f"{PASTA_META_ONS}{arq.lower()}"
        if not arquivo_ja_existe(chave_dest_meta):
            try:
                s3.copy_object(CopySource={'Bucket': 'ons-aws-prod-opendata', 'Key': f"dataset/dados_hidrologicos_di/{arq}"},
                               Bucket=BUCKET_DESTINO, Key=chave_dest_meta)
                logger.info(f" -> ONS Metadados ({arq}): Sucesso!")
            except Exception as e: 
                erros_pipeline.append(f"Fase 5 (ONS Meta {arq}): {e}")

    # 5.1.x Copiar quaisquer .json/.pdf presentes nos prefixes ONS para metadata por dataset
    for prefix in ONS_PREFIXOS_BRONZE:
        resp = s3.list_objects_v2(Bucket='ons-aws-prod-opendata', Prefix=prefix)
        dataset_name = prefix.rstrip('/').split('/')[-1]
        for obj in resp.get('Contents', []):
            key = obj['Key']
            key_lower = key.lower()
            if not (key_lower.endswith('.json') or key_lower.endswith('.pdf')):
                continue
            filename = os.path.basename(key_lower)
            chave_dest_meta = f"{PASTA_META_ONS_ROOT}{dataset_name}/{filename}"
            if arquivo_ja_existe(chave_dest_meta):
                logger.info(f" -> ONS Meta {dataset_name}/{filename}: já existe. Pulando.")
                continue
            try:
                s3.copy_object(CopySource={'Bucket': 'ons-aws-prod-opendata', 'Key': key},
                               Bucket=BUCKET_DESTINO, Key=chave_dest_meta)
                logger.info(f" -> ONS Metadados ({dataset_name}/{filename}): Sucesso!")
            except Exception as e:
                erros_pipeline.append(f"Fase 5 (ONS Meta {dataset_name}/{filename}): {e}")

    # 5.2 INMET Microdados
    chave_dest_inmet_meta = f"{PASTA_META_INMET_MICRO}dicionario_inmet_microdados.json"
    if not arquivo_ja_existe(chave_dest_inmet_meta):
        try:
            tabela_bq = bq_client.get_table('basedosdados.br_inmet_bdmep.microdados')
            dict_inmet = {"titulo": "Dicionario INMET Microdados", "campos":[]}
            for campo in tabela_bq.schema:
                dict_inmet["campos"].append({"codigo": campo.name, "tipo": campo.field_type, "descricao": campo.description})

            s3.put_object(Bucket=BUCKET_DESTINO, Key=chave_dest_inmet_meta, Body=json.dumps(dict_inmet, indent=4, ensure_ascii=False))
            logger.info(" -> INMET Microdados Metadados: Sucesso!")
        except Exception as e:
            erros_pipeline.append(f"Fase 5 (INMET Micro Meta): {e}")

    # 5.3 INMET Estações
    chave_dest_inmet_est = f"{PASTA_META_INMET_EST}dicionario_inmet_estacoes.json"
    if not arquivo_ja_existe(chave_dest_inmet_est):
        try:
            csv_obj = s3.get_object(Bucket=BUCKET_DESTINO, Key=f"{PASTA_INMET_EST}inmet_estacoes_raw.csv")
            df_est = pd.read_csv(io.BytesIO(csv_obj['Body'].read()), encoding='utf-8')
            
            tipo_mapa = {'object': 'STRING', 'int64': 'INTEGER', 'int32': 'INTEGER', 'float64': 'FLOAT', 'float32': 'FLOAT', 'bool': 'BOOLEAN', 'datetime64[ns]': 'DATE'}
            dict_est = {"titulo": "Dicionario INMET Estacoes", "campos":[]}
            
            for col_name in df_est.columns:
                tipo_pandas = str(df_est[col_name].dtype)
                dict_est["campos"].append({"codigo": col_name, "tipo": tipo_mapa.get(tipo_pandas, 'STRING'), "descricao": f"Campo {col_name}"})
            
            del df_est
            gc.collect()
            
            s3.put_object(Bucket=BUCKET_DESTINO, Key=chave_dest_inmet_est, Body=json.dumps(dict_est, indent=4, ensure_ascii=False))
            logger.info(" -> INMET Estações Metadados: Sucesso!")
        except Exception as e:
            erros_pipeline.append(f"Fase 5 (INMET Estações Meta): {e}")

    # 5.4 Diretórios Brasil (Municípios)
    chave_dest_mun_meta = f"{PASTA_META_DIR_MUN}dicionario_diretorios_municipios.json"
    if not arquivo_ja_existe(chave_dest_mun_meta):
        try:
            tabela_mun_bq = bq_client.get_table('basedosdados.br_bd_diretorios_brasil.municipio')
            dict_mun = {"titulo": "Dicionario Diretorios Brasil - Municipios", "campos":[]}
            for campo in tabela_mun_bq.schema:
                dict_mun["campos"].append({"codigo": campo.name, "tipo": campo.field_type, "descricao": campo.description})

            s3.put_object(Bucket=BUCKET_DESTINO, Key=chave_dest_mun_meta, Body=json.dumps(dict_mun, indent=4, ensure_ascii=False))
            logger.info(" -> Diretórios Municípios Metadados: Sucesso!")
        except Exception as e:
            erros_pipeline.append(f"Fase 5 (Diretórios Municípios Meta): {e}")

    logger.info("\n==============================================")
    logger.info(">>> PIPELINE BRONZE FINALIZADO <<<")
    
    if len(erros_pipeline) > 0:
        logger.warning(f"Ocorreram {len(erros_pipeline)} erros durante a execução:")
        for erro in erros_pipeline:
            logger.warning(f" - {erro}")
    else:
        logger.info("Todos os processos foram concluídos com 100% de SUCESSO.")
    logger.info("==============================================\n")

if __name__ == "__main__":
    executar_ingestao_full_bronze()