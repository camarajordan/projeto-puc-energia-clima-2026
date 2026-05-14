# Projeto PUC Energia Clima 2026

Pipeline de dados em 3 camadas (Bronze, Silver, Gold) para analise integrada de dados climaticos e eletricos.

## Objetivo

Unificar dados de fontes publicas (ONS e INMET), padronizar os dados no Data Lake e disponibilizar modelo analitico na camada Gold para consumo em BI.

## Arquitetura

- Bronze: extracao e espelhamento dos dados brutos em S3
- Silver: limpeza, tipagem e padronizacao (AWS Glue / Spark)
- Gold: modelagem analitica com SQL no Athena (dimensoes + fatos)

## Estrutura do repositorio

- bronze/ingestao.py
- silver/silver_glue_final.py
- gold/orquestrador_gold.py
- gold/scripts/*.sql
- metadata/
- utils/
- docs/
- scripts/

## Pre requisitos

- Conta AWS com permissao para S3, Athena e Glue
- Conta GCP com BigQuery habilitado
- Python 3.10+
- AWS CLI configurado
- Permissoes para criar bucket e database Athena

## Mini tutorial BigQuery (obrigatorio para replicacao)

A extracao da fonte INMET no script da camada Bronze usa BigQuery. Cada pessoa deve criar e usar as proprias credenciais.

1. Crie (ou use) um projeto no GCP.
2. Ative a API do BigQuery no projeto.
3. Crie uma Service Account para o projeto.
4. Conceda, no minimo, permissao de leitura no BigQuery (exemplo: BigQuery Data Viewer e BigQuery Job User).
5. Gere uma chave JSON da Service Account.
6. Salve o arquivo JSON localmente (nao commitar no Git).
7. Configure a variavel de ambiente:

Windows PowerShell:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\caminho\para\sua-chave.json"
```

Linux/CloudShell:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/home/cloudshell-user/sua-chave.json"
```

8. Ajuste o script bronze/ingestao.py para usar esta variavel (se necessario) e nao um nome fixo de arquivo.

## O que era manual e agora deve ficar reproduzivel

Historico de passos manuais identificados:

- Criacao de bucket S3
- Criacao de database no Athena

Para facilitar replicacao, estes passos podem ser feitos via CloudShell.

## Preparação manual (S3 + Athena)

Observação: prepare manualmente o bucket S3 e o database do Athena no ambiente onde vai executar o pipeline (por exemplo, CloudShell). O repositório inclui um script opcional `scripts/bootstrap_cloudshell_aws.sh`, mas não é obrigatório.

## Ordem de execucao

1. Rodar bootstrap no CloudShell (bucket + database)
2. Rodar camada Bronze (bronze/ingestao.py)
3. Rodar camada Silver (silver/silver_glue_final.py no Glue)
4. Rodar camada Gold (gold/orquestrador_gold.py)

## Seguranca

Nunca versionar credenciais.

Itens bloqueados no .gitignore:

- myb2bapp-405901-fee9b119d4d9.json
- arquivos .pem e .key
- arquivos .env

## Proximos passos

- Documentar parametros de execucao de cada camada
- Remover hardcode de credenciais do script Bronze (usar apenas variavel de ambiente)
- Automatizar upload dos SQLs para S3 antes da execucao Gold
