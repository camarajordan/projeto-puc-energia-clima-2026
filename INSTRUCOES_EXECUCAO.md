# Guia de Execução (Passo a Passo)

Este documento contém as instruções para replicar a infraestrutura e executar o pipeline de dados na AWS.

## 0. Pré-requisitos (Configurações Cloud)

### Google Cloud Platform (GCP)
1. Crie um projeto no GCP e anote o Project ID.
2. Acesse IAM e administrador > Contas de Serviço e crie uma nova conta com permissão de leitura no BigQuery.
3. Na aba Chaves dessa conta, crie uma nova chave em formato JSON e baixe para sua máquina.

### Amazon Web Services (AWS)
1. Crie um bucket no S3 chamado `projeto-puc-energia-clima-2026` ou ajuste a variável `BUCKET_DESTINO` nos scripts caso use outro nome.
2. Acesse o AWS CloudShell.
3. Faça upload do arquivo JSON de credenciais do GCP para o CloudShell.

---

## 1. Configuração do Ambiente e Instalação de Dependências

No terminal do AWS CloudShell, instale as bibliotecas necessárias:

```bash
pip3 install --no-cache-dir basedosdados pyarrow db-dtypes requests
```

---

## 2. Execução da Camada Bronze (Ingestão)

1. Faça upload do arquivo `bronze/ingestao.py` para o CloudShell.
2. Abra o script e confirme se a variável `GOOGLE_APPLICATION_CREDENTIALS` aponta para o seu arquivo JSON do GCP. Se a sua cópia do script exigir, ajuste também o Project ID.
3. Execute a ingestão:

```bash
python3 ingestao.py
```

Os arquivos Parquet serão salvos no S3 nas pastas raw da camada Bronze.

---

## 3. Execução da Camada Silver (Processamento Spark)

1. No console da AWS, busque por AWS Glue e acesse ETL jobs.
2. Clique em Script editor, selecione a engine Spark e crie o job.
3. Copie o conteúdo de `silver/silver_glue.py` e cole no editor.
4. Na aba Job details, configure:
   - IAM Role: `LabRole`
   - Worker type: `G 1X`
   - Requested number of workers: `2`
5. Salve e clique em Run. Aguarde o status `Succeeded`.

### Catalogando os dados

1. Ainda no AWS Glue, vá em Crawlers e crie um novo crawler.
2. Aponte para a pasta S3 `s3://projeto-puc-energia-clima-2026/silver/`.
3. Escolha a `LabRole` e adicione a um novo banco de dados, por exemplo `db_energia_clima_puc`.
4. Execute o crawler.

---

## 4. Execução da Camada Gold (Modelo Dimensional Athena)

1. Faça o upload dos arquivos `.sql` da pasta `gold/scripts/` para o S3, se o seu orquestrador exigir leitura remota.
2. Faça o upload do orquestrador `gold/orquestrador_gold.py` para o AWS CloudShell.
3. Execute o orquestrador:

```bash
python3 orquestrador_gold.py
```

O script executa as queries SQL no Athena, respeitando a ordem de dependências do modelo.

