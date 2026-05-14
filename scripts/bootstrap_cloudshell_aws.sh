#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
BUCKET="projeto-puc-energia-clima-2026"
DATABASE="db_energia_clima_puc"
ATHENA_OUTPUT="s3://${BUCKET}/athena-results/"

echo "[1/3] Verificando bucket ${BUCKET}..."
if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  echo "Bucket ja existe: ${BUCKET}"
else
  echo "Criando bucket: ${BUCKET}"
  if [[ "${REGION}" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}"
  else
    aws s3api create-bucket \
      --bucket "${BUCKET}" \
      --region "${REGION}" \
      --create-bucket-configuration LocationConstraint="${REGION}"
  fi
fi

echo "[2/3] Garantindo prefixo de resultados do Athena..."
aws s3 cp /dev/null "${ATHENA_OUTPUT}.keep" >/dev/null

echo "[3/3] Criando database no Athena (idempotente)..."
QUERY="CREATE DATABASE IF NOT EXISTS ${DATABASE};"
EXEC_ID=$(aws athena start-query-execution \
  --region "${REGION}" \
  --query-string "${QUERY}" \
  --result-configuration OutputLocation="${ATHENA_OUTPUT}" \
  --query 'QueryExecutionId' --output text)

echo "QueryExecutionId: ${EXEC_ID}"
echo "Ambiente inicial AWS pronto."
