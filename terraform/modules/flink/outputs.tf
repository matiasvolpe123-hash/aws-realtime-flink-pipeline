output "application_name" { value = aws_kinesisanalyticsv2_application.flink.name }
output "log_group_name" { value = aws_cloudwatch_log_group.flink.name }
