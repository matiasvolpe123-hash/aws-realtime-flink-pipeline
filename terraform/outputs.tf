output "kinesis_stream_name" { value = module.ingestion.stream_name }
output "firehose_name" { value = module.ingestion.firehose_name }
output "flink_application_name" { value = module.flink.application_name }
output "flink_log_group_name" { value = module.flink.log_group_name }
output "datalake_bucket_name" { value = module.storage.bucket_id }

output "redshift_analytics_role_arn" {
  description = "ARN del rol IAM usado por Redshift para Streaming Ingestion y Spectrum"
  value       = module.redshift.iam_role_arn
}

output "redshift_workgroup_endpoint" { value = module.redshift.workgroup_endpoint }
output "redshift_admin_username" { value = module.redshift.admin_username }
output "redshift_iam_role_arn" { value = module.redshift.iam_role_arn }

output "redshift_admin_password" {
  value     = module.redshift.admin_password
  sensitive = true
}
