#!/usr/bin/env bash
set -euo pipefail

region="${AWS_REGION:-us-east-2}"
application="${1:-realtime-sensors-dev-flink-aggregation}"
output="${2:-evidence/flink-cloudwatch.txt}"
log_group="/aws/kinesis-analytics/${application}"

mkdir -p "$(dirname "$output")"
{
  printf 'Captured at (UTC): %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf 'AWS region: %s\n' "$region"
  printf 'Application: %s\n\n' "$application"
  printf '%s\n' '=== Managed Flink application status ==='
  aws kinesisanalyticsv2 describe-application \
    --application-name "$application" \
    --query 'ApplicationDetail.{Name:ApplicationName,Status:ApplicationStatus,Runtime:RuntimeEnvironment}' \
    --output table --region "$region"
  printf '\n%s\n' '=== CloudWatch logs (last 15 minutes) ==='
  aws logs tail "$log_group" --since 15m --format short --region "$region"
} 2>&1 | tee "$output"

printf '\nEvidence saved to %s\n' "$output"
