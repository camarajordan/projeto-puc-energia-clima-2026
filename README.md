# Análise Integrada de Dados Climáticos e Elétricos no Brasil

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)

> Projeto de Big Data Analytics — PUC Minas · Eixo 4 · 2026/1  
> Curso de Tecnologia em Banco de Dados

### [Clique aqui para acessar o guia passo a passo de replicação do projeto](./instrucoes_replicacao_projeto.md)

---

## Resumo do Projeto

O objetivo deste projeto é analisar o impacto das variáveis climáticas (precipitação, vento, temperatura e radiação solar) na geração de energia (hidrelétrica, eólica, solar e térmica) e na demanda/carga do Sistema Elétrico Brasileiro.

Para isso, foi construído um **Data Lake na AWS** utilizando a **Arquitetura Medalhão (Bronze, Silver, Gold)**. O pipeline integra dados maciços de duas instituições principais:

- **ONS (Operador Nacional do Sistema Elétrico):** Geração, capacidade e carga de energia.
- **INMET (Instituto Nacional de Meteorologia):** Microdados climáticos horários de estações de todo o país.

O cliente fictício é o **Operador Nacional do Sistema Elétrico (ONS)**, e a questão central de análise é:

> *"Como o desempenho operacional dos reservatórios e usinas hidrelétricas impacta a eficiência da geração de energia, e de que forma a análise desses dados pode apoiar decisões mais eficazes na gestão dos recursos hídricos?"*

O recorte temporal da análise abrange **9 anos (2016 a 2024)**.

### 📖 Fontes de Dados

| Fonte | Tipo | Dados |
|-------|------|-------|
| **ONS** (Operador Nacional do Sistema Elétrico) | Público / S3 | Hidrologia, carga, geração por usina, capacidade instalada |
| **INMET** (Instituto Nacional de Meteorologia) | Público / BigQuery | Microdados de estações meteorológicas (temperatura, vento, chuva, radiação) |
| **IBGE** | Público / API | Diretório de municípios brasileiros (dimensão geográfica) |

---

## 🏗️ Arquitetura de Dados

O pipeline foi projetado para contornar limitações severas de hardware e custos do ambiente acadêmico AWS Learner Lab. O projeto segue a **Medallion Architecture** (Bronze → Silver → Gold), hospedada integralmente na AWS.

```text
┌─────────────────────────────────────────────────────────┐
│                    FONTES EXTERNAS                      │
│       ONS (público)    INMET (BigQuery)    IBGE         │
└──────────────────────┬──────────────────────────────────┘
		       │
		       ▼
	      ┌─────────────────┐
	      │     BRONZE      │  ingestao.py
	      │   (Amazon S3)   │  Dados brutos em Parquet
	      └────────┬────────┘
		       │
		       ▼
	      ┌─────────────────┐
			  │     SILVER      │  silver_glue.py
	      │  (AWS Glue /    │  Limpeza, tipagem e joins
	      │   Amazon S3)    │  Particionado por year/month
	      └────────┬────────┘
		       │
		       ▼
	      ┌─────────────────────┐
	      │       GOLD          │  backfill_carga_historica.py
	      │  (Amazon Athena)    │  Star Schema — 10 tabelas
	      │  Star Schema        │  Dims + Fatos (2016–2024)
	      └────────┬────────────┘
		       │
		       ▼
	       ┌───────────────┐
	       │   Power BI    │  Dashboards e análises
	       └───────────────┘
```

### 🚀 🥉 1. Bronze — Ingestão (`bronze/ingestao.py`)
- **ONS:** Cópia direta entre buckets S3 (nuvem a nuvem).
- **INMET:** Estratégia *Multi-Cloud*. Os dados (12+ GB) são consultados no Google BigQuery e convertidos para Parquet utilizando **In-Memory Streaming** direto para o S3. *Isso foi feito para contornar o limite restrito de 1GB de disco do AWS CloudShell e evitar o encerramento forçado (`Killed`) do processo por falta de memória RAM.*
- **Governança:** Geração dinâmica de dicionários de dados (JSON) a partir dos schemas de origem.

### 🥈 2. Silver — Transformação (`silver/silver_glue.py`)
Job PySpark no AWS Glue adotando a estratégia *Lean Silver* (filtrando apenas colunas essenciais para otimizar custo e performance).
- Conversão do padrão decimal brasileiro (vírgula → ponto) e tratamento de nulos.
- Normalização de tipos incompatíveis do BigQuery (ex: `TIME` → `STRING`).
- Conversão de fuso horário UTC → `America/Sao_Paulo`.
- Agregação dos dados horários climáticos para granularidade diária.
- Armazenamento em Parquet (compressão Snappy), particionado por `year=/month=/`.

### 🥇 3. Gold — Modelo Analítico (`gold/backfill_carga_historica.py`)
Script de Automação de Carga Histórica (Backfill) Python que executa os 10 scripts SQL no Amazon Athena, construindo o **Star Schema**.
- **Estratégia:** Para contornar o limite de 100 partições concorrentes do Athena, aplicou-se o padrão *Subquery Wrapper* usando `CTAS` para o ano base de 2016, seguido de `INSERT INTO` iterativos para os anos subsequentes.
- O resultado é um modelo composto por Dimensões (Tempo, Localização, Usina) e Fatos (Clima, Geração, Hidrologia, Carga e Eficiência).

---

**Professor Tutor:** Prof. Marco Paulo Soares Gomes  
**Instituição:** PUC Minas — Tecnologia em Banco de Dados (2026/1)
