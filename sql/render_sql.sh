#!/usr/bin/env bash
# Sustituye el placeholder __REDSHIFT_IAM_ROLE_ARN__ por el ARN real,
# obtenido del output de Terraform (nunca hardcodeado en el repo).
set -euo pipefail
cd "$(dirname "$0")/../terraform"
ROLE_ARN=$(terraform output -raw redshift_analytics_role_arn)
cd ..
sed "s|__REDSHIFT_IAM_ROLE_ARN__|${ROLE_ARN}|g" sql/streaming_analytics.sql > sql/streaming_analytics.rendered.sql
echo "Generado: sql/streaming_analytics.rendered.sql"
