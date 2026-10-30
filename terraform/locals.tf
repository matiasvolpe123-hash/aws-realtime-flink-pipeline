data "aws_caller_identity" "current" {}

locals {
  prefix           = "${var.project_name}-${var.environment}"
  bucket_name      = "${local.prefix}-${data.aws_caller_identity.current.account_id}"
  stream_name      = "${local.prefix}-events"
  application_name = "${local.prefix}-flink-aggregation"
  code_key         = "applications/flink/flink-kinesis-processor.jar"
}
