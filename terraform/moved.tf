# Mapea los recursos del código plano anterior a su nueva ubicación en módulos,
# para que Terraform los mueva en el estado en vez de destruirlos y recrearlos.

moved {
  from = aws_s3_bucket.lake
  to   = module.storage.aws_s3_bucket.lake
}

moved {
  from = aws_s3_bucket_versioning.lake
  to   = module.storage.aws_s3_bucket_versioning.lake
}

moved {
  from = aws_s3_bucket_public_access_block.lake
  to   = module.storage.aws_s3_bucket_public_access_block.lake
}

moved {
  from = aws_s3_bucket_server_side_encryption_configuration.lake
  to   = module.storage.aws_s3_bucket_server_side_encryption_configuration.lake
}

moved {
  from = aws_kms_key.stream
  to   = module.storage.aws_kms_key.stream
}

moved {
  from = aws_kms_alias.stream
  to   = module.storage.aws_kms_alias.stream
}

moved {
  from = aws_glue_catalog_database.lakehouse_db
  to   = module.storage.aws_glue_catalog_database.lakehouse_db
}

moved {
  from = aws_kinesis_stream.events
  to   = module.ingestion.aws_kinesis_stream.events
}

moved {
  from = aws_cloudwatch_log_group.firehose
  to   = module.ingestion.aws_cloudwatch_log_group.firehose
}

moved {
  from = aws_cloudwatch_log_stream.firehose
  to   = module.ingestion.aws_cloudwatch_log_stream.firehose
}

moved {
  from = aws_iam_role.firehose
  to   = module.ingestion.aws_iam_role.firehose
}

moved {
  from = aws_iam_role_policy.firehose
  to   = module.ingestion.aws_iam_role_policy.firehose
}

moved {
  from = aws_kinesis_firehose_delivery_stream.raw
  to   = module.ingestion.aws_kinesis_firehose_delivery_stream.raw
}

moved {
  from = aws_cloudwatch_log_stream.flink
  to   = module.flink.aws_cloudwatch_log_stream.flink
}

moved {
  from = aws_cloudwatch_log_group.flink
  to   = module.flink.aws_cloudwatch_log_group.flink
}

moved {
  from = aws_s3_object.flink_code
  to   = module.flink.aws_s3_object.flink_code
}

moved {
  from = aws_iam_role.flink
  to   = module.flink.aws_iam_role.flink
}

moved {
  from = aws_iam_role_policy.flink
  to   = module.flink.aws_iam_role_policy.flink
}

moved {
  from = aws_kinesisanalyticsv2_application.flink
  to   = module.flink.aws_kinesisanalyticsv2_application.flink
}

moved {
  from = aws_security_group.redshift
  to   = module.redshift.aws_security_group.redshift
}

moved {
  from = random_password.redshift_admin
  to   = module.redshift.random_password.redshift_admin
}

moved {
  from = aws_iam_role.redshift
  to   = module.redshift.aws_iam_role.redshift
}

moved {
  from = aws_iam_role_policy.redshift_analytics
  to   = module.redshift.aws_iam_role_policy.redshift_analytics
}

moved {
  from = aws_redshiftserverless_namespace.analytics
  to   = module.redshift.aws_redshiftserverless_namespace.analytics
}

moved {
  from = aws_redshiftserverless_workgroup.analytics
  to   = module.redshift.aws_redshiftserverless_workgroup.analytics
}
