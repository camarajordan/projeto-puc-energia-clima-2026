import boto3
import time
import sys
import re

# =============================================================================
# Configurações
# =============================================================================
BUCKET         = 'projeto-puc-energia-clima-2026'
PREFIX_SCRIPTS = 'gold/scripts/'
ATHENA_RESULTS = f's3://{BUCKET}/athena-results/'
DATABASE       = 'db_energia_clima_puc'
POLL_INTERVAL  = 5  # segundos entre checagens de status

# Scripts de fato: usam estratégia CTAS(2016) + INSERT INTO por ano
SCRIPTS_FATO = {
    '04_gold_fato_clima.sql',
    '05_gold_fato_geracao.sql',
    '06_gold_fato_hidrologia.sql',
    '07_gold_fato_carga.sql',
    '08_gold_fato_eficiencia_hidro.sql',
    '09_gold_bridge_clima_geracao.sql',
}

# Erros que não interrompem o pipeline (apenas aviso)
ERROS_IGNORAVEIS = [
    'TABLE_ALREADY_EXISTS',
    'AlreadyExistsException',
]

ANOS = list(range(2016, 2025))  # 2016 a 2024

s3     = boto3.client('s3')
athena = boto3.client('athena')


# =============================================================================
# Utilitários SQL
# =============================================================================
def strip_comentarios(sql):
    """
    Remove linhas que são puramente comentários SQL (--)
    para evitar que o split por ';' cole comentários no início
    do bloco seguinte, mascarando o tipo real do comando.
    """
    linhas_limpas = []
    for linha in sql.splitlines():
        stripped = linha.strip()
        if not stripped.startswith('--'):
            linhas_limpas.append(linha)
    return '\n'.join(linhas_limpas)


def split_comandos(sql):
    """
    Remove comentários, divide por ';' e retorna lista de comandos não-vazios.
    """
    sql_limpo = strip_comentarios(sql)
    return [cmd.strip() for cmd in sql_limpo.split(';') if cmd.strip()]


def primeiro_token(cmd):
    tokens = cmd.split()
    return tokens[0].upper() if tokens else ''


def extrair_nome_tabela(query_sql):
    """Extrai o nome da tabela do CREATE TABLE."""
    for linha in query_sql.splitlines():
        partes = linha.strip().split()
        if len(partes) >= 3 and partes[0].upper() == 'CREATE' and partes[1].upper() == 'TABLE':
            return partes[2].strip().lower()
    return None


# =============================================================================
# Execução no Athena
# =============================================================================
def executar_query_athena(query_string, descricao):
    """
    Envia query ao Athena e aguarda conclusão.
    Retorna: 'ok', 'ignorado' (erro tolerável) ou 'erro' (fatal).
    """
    response = athena.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_RESULTS}
    )
    query_id = response['QueryExecutionId']
    inicio   = time.time()

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        estado = status['QueryExecution']['Status']['State']

        if estado == 'SUCCEEDED':
            elapsed = round(time.time() - inicio, 1)
            print(f"    ✓ SUCESSO em {elapsed}s  [{descricao}]")
            return 'ok'

        if estado in ('FAILED', 'CANCELLED'):
            erro = status['QueryExecution']['Status'].get('StateChangeReason', 'sem detalhes')

            # Verifica se é um erro tolerável (ex: tabela já existe)
            for ignoravel in ERROS_IGNORAVEIS:
                if ignoravel in erro:
                    print(f"    ⚠ IGNORADO ({ignoravel}) — pulando para o próximo.  [{descricao}]")
                    return 'ignorado'

            print(f"    ✗ {estado}: {erro}  [{descricao}]")
            return 'erro'

        time.sleep(POLL_INTERVAL)


# =============================================================================
# Execução de scripts normais (dimensões, database)
# =============================================================================
def executar_script_normal(query_sql, nome_arquivo):
    comandos = split_comandos(query_sql)
    total    = len(comandos)

    for i, cmd in enumerate(comandos, start=1):
        tipo = primeiro_token(cmd)
        print(f"  Comando {i}/{total} ({tipo})...")
        resultado = executar_query_athena(cmd, nome_arquivo)
        if resultado == 'erro':
            return False
    return True


# =============================================================================
# Execução de scripts de fato (CTAS ano 2016 + INSERT INTO por ano)
# =============================================================================
def executar_script_fato(query_sql, nome_arquivo):
    """
    Contorna o limite de 100 partições simultâneas do Athena:
      1. DROP TABLE
      2. CTAS filtrado para year='2016' (≤12 partições abertas)
      3. INSERT INTO para 2017..2024 (≤12 partições por execução)
    """
    nome_tabela = extrair_nome_tabela(query_sql)
    if not nome_tabela:
        print(f"  ⚠ Não consegui extrair nome da tabela. Rodando como script normal.")
        return executar_script_normal(query_sql, nome_arquivo)

    comandos   = split_comandos(query_sql)
    cmd_drop   = next((c for c in comandos if primeiro_token(c) == 'DROP'),   None)
    cmd_create = next((c for c in comandos if primeiro_token(c) == 'CREATE'), None)

    # Passo 1: DROP
    if cmd_drop:
        print(f"  DROP TABLE...")
        resultado = executar_query_athena(cmd_drop, f"{nome_arquivo} / DROP")
        if resultado == 'erro':
            return False

    if not cmd_create:
        print(f"  ⚠ Nenhum CREATE TABLE encontrado em {nome_arquivo}.")
        return False

    # Passo 2: CTAS restrito ao primeiro ano
    primeiro_ano  = str(ANOS[0])
    ctas_filtrado = gerar_ctas_com_filtro(cmd_create, nome_tabela, primeiro_ano)
    print(f"  CTAS ano={primeiro_ano} (cria tabela + primeiras partições)...")
    resultado = executar_query_athena(ctas_filtrado, f"{nome_arquivo} / CTAS {primeiro_ano}")
    if resultado == 'erro':
        return False

    # Passo 3: INSERT INTO para anos restantes
    for ano in ANOS[1:]:
        insert_sql = gerar_insert_into(cmd_create, nome_tabela, str(ano))
        print(f"  INSERT INTO ano={ano}...")
        resultado = executar_query_athena(insert_sql, f"{nome_arquivo} / INSERT {ano}")
        if resultado == 'erro':
            return False

    return True


# =============================================================================
# Manipulação de SQL para injeção de filtros de ano (Subquery Wrapper)
# =============================================================================


def extrair_cabecalho_e_corpo(ctas_sql):
    """
    Separa CREATE TABLE ... AS do corpo (WITH...SELECT) usando regex.
    Retorna: (cabecalho, corpo) ou (None, None) se falhar.
    """
    match = re.search(r"(.*?\)\s*AS\s+)(.*)", ctas_sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return None, None
    return match.group(1).strip(), match.group(2).strip()


def gerar_ctas_com_filtro(ctas_sql, nome_tabela, ano):
    """
    Gera CREATE TABLE ... AS SELECT * FROM (corpo) AS subq WHERE year = '<ano>'.
    Usa Subquery Wrapper para máxima segurança.
    """
    cabecalho, corpo = extrair_cabecalho_e_corpo(ctas_sql)
    if not cabecalho or not corpo:
        # fallback: adicionar filtro simples ao final
        return ctas_sql.rstrip().rstrip(';') + f"\nWHERE year = '{ano}'"
    
    return f"{cabecalho}\nSELECT * FROM (\n{corpo}\n) AS subq WHERE year = '{ano}'"


def gerar_insert_into(ctas_sql, nome_tabela, ano):
    """
    Gera INSERT INTO <tabela> SELECT * FROM (corpo) AS subq WHERE year = '<ano>'.
    Usa Subquery Wrapper para máxima segurança.
    """
    _, corpo = extrair_cabecalho_e_corpo(ctas_sql)
    if not corpo:
        # fallback
        return f"INSERT INTO {nome_tabela}\nSELECT * FROM ({ctas_sql.rstrip().rstrip(';')}) AS subq WHERE year = '{ano}'"
    
    return f"INSERT INTO {nome_tabela}\nSELECT * FROM (\n{corpo}\n) AS subq WHERE year = '{ano}'"


# =============================================================================
# Pipeline principal
# =============================================================================
def listar_scripts_s3():
    paginator = s3.get_paginator('list_objects_v2')
    chaves    = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_SCRIPTS):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.sql'):
                chaves.append(obj['Key'])
    return sorted(chaves)


def ler_sql_s3(chave):
    obj = s3.get_object(Bucket=BUCKET, Key=chave)
    return obj['Body'].read().decode('utf-8')


def rodar_pipeline_gold():
    print("=" * 60)
    print("  Orquestração da Camada Gold — Amazon Athena")
    print(f"  Anos: {ANOS[0]} a {ANOS[-1]}")
    print("  Fatos: CTAS(2016) + INSERT INTO por ano")
    print("  TABLE_ALREADY_EXISTS: ignorado (pipeline idempotente)")
    print("=" * 60)

    scripts = listar_scripts_s3()
    if not scripts:
        print("Nenhum script .sql encontrado em:", PREFIX_SCRIPTS)
        sys.exit(1)

    print(f"\n{len(scripts)} script(s) encontrado(s):\n")
    for s in scripts:
        nome = s.split('/')[-1]
        tipo = "fato (CTAS+INSERT)" if nome in SCRIPTS_FATO else "dimensão/setup"
        print(f"  • {nome}  [{tipo}]")
    print()

    for chave_script in scripts:
        nome_arquivo = chave_script.split('/')[-1]
        print(f"[{nome_arquivo}]")

        query_sql = ler_sql_s3(chave_script)

        if nome_arquivo in SCRIPTS_FATO:
            ok = executar_script_fato(query_sql, nome_arquivo)
        else:
            ok = executar_script_normal(query_sql, nome_arquivo)

        if not ok:
            print(f"\n>>> PIPELINE INTERROMPIDO em [{nome_arquivo}] <<<")
            print("Corrija o erro acima e reexecute a partir deste script.")
            sys.exit(1)

        print()

    print("=" * 60)
    print("  PIPELINE GOLD CONCLUÍDO COM SUCESSO!")
    print("  Abra o Athena ou o Power BI para ver as tabelas.")
    print("=" * 60)


if __name__ == "__main__":
    rodar_pipeline_gold()
