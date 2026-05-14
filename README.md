# Análise Integrada de Dados Climáticos e Elétricos no Brasil

> Projeto de Big Data Analytics - PUC Minas - Eixo 4 - 2026/1

Este repositório contém o pipeline de dados em três camadas para integrar dados climáticos e elétricos com a arquitetura Medallion:

- Bronze: ingestão e espelhamento dos dados brutos em S3
- Silver: limpeza, tipagem e padronização com AWS Glue / Spark
- Gold: modelagem analítica com SQL no Athena

Para executar o projeto, consulte o guia completo em [INSTRUCOES_EXECUCAO.md](INSTRUCOES_EXECUCAO.md).

## Resumo do Projeto

O objetivo é analisar a relação entre variáveis climáticas e a geração, carga e hidrologia do sistema elétrico brasileiro, com foco em dados públicos do ONS, INMET e IBGE.

## Arquitetura

O projeto foi organizado para operar em um ambiente acadêmico com recursos limitados, mantendo a separação entre ingestão, transformação e modelagem analítica.

```text
Fontes externas -> Bronze (S3) -> Silver (Glue/S3) -> Gold (Athena) -> Consumo analítico
```

## Estrutura principal

- `bronze/ingestao.py`
- `silver/silver_glue_final.py`
- `gold/orquestrador_gold.py`
- `gold/scripts/*.sql`
- `metadata/`
- `utils/`

## Fontes de Dados

- ONS: dados de geração, capacidade, carga e hidrologia
- INMET: microdados meteorológicos via BigQuery
- IBGE / diretórios: base geográfica e apoio à modelagem

## Observação

As instruções operacionais, pré-requisitos e sequência de execução estão separadas em [INSTRUCOES_EXECUCAO.md](INSTRUCOES_EXECUCAO.md).
