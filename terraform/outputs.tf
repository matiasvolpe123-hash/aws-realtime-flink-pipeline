output "kinesis_stream_name" { value = aws_kinesis_stream.events.name }
output "firehose_name" { value = aws_kinesis_firehose_delivery_stream.raw.name }
output "flink_application_name" { value = aws_kinesisanalyticsv2_application.flink.name }
output "flink_log_group_name" { value = aws_cloudwatch_log_group.flink.name }
output "datalake_bucket_name" { value = aws_s3_bucket.lake.id }
