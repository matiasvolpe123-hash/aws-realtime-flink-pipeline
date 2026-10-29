data "aws_caller_identity" "current" {}

locals {
  prefix           = "${var.project_name}-${var.environment}"
  bucket_name      = "${local.prefix}-${data.aws_caller_identity.current.account_id}"
  stream_name      = "${local.prefix}-events"
  application_name = "${local.prefix}-flink-aggregation"
  code_key         = "applications/flink/flink-kinesis-processor.jar"
}

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_kms_key" "stream" {
  description             = "KMS key for ${local.stream_name}"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "stream" {
  name          = "alias/${local.prefix}-stream"
  target_key_id = aws_kms_key.stream.key_id
}

resource "aws_kinesis_stream" "events" {
  name             = local.stream_name
  shard_count      = var.shard_count
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = aws_kms_key.stream.arn
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.prefix}-to-s3"
  retention_in_days = 14
}

resource "aws_iam_role" "firehose" {
  name = "${local.prefix}-firehose-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "firehose.amazonaws.com" },
    Action = "sts:AssumeRole", Condition = { StringEquals = { "sts:ExternalId" = data.aws_caller_identity.current.account_id } } }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  name = "${local.prefix}-firehose-policy"
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["kinesis:DescribeStream", "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards"], Resource = aws_kinesis_stream.events.arn },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = aws_kms_key.stream.arn },
      { Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"], Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/streaming/raw/*"] },
      { Effect = "Allow", Action = ["logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.firehose.arn}:*" }
    ]
  })
}

resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = "${local.prefix}-to-s3"
  destination = "extended_s3"
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events.arn
    role_arn           = aws_iam_role.firehose.arn
  }
  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose.arn
    bucket_arn         = aws_s3_bucket.lake.arn
    prefix             = "streaming/raw/"
    buffering_size     = 5
    buffering_interval = 60
    compression_format = "UNCOMPRESSED"
    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = "S3Delivery"
    }
  }
  depends_on = [aws_iam_role_policy.firehose]
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${local.application_name}"
  retention_in_days = 14
}

resource "aws_s3_object" "flink_code" {
  bucket       = aws_s3_bucket.lake.id
  key          = local.code_key
  source       = var.artifact_path
  source_hash  = filebase64sha256(var.artifact_path)
  content_type = "application/java-archive"
}

resource "aws_iam_role" "flink" {
  name = "${local.prefix}-flink-execution-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "kinesisanalytics.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "flink" {
  name = "${local.prefix}-flink-runtime-policy"
  role = aws_iam_role.flink.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards"], Resource = aws_kinesis_stream.events.arn },
      { Effect = "Allow", Action = ["kinesis:ListStreams"], Resource = "*" },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = aws_kms_key.stream.arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = aws_s3_object.flink_code.arn },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:DescribeLogStreams", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.flink.arn}:*" }
    ]
  })
}

resource "aws_kinesisanalyticsv2_application" "flink" {
  name                   = local.application_name
  runtime_environment    = "FLINK-1_18"
  service_execution_role = aws_iam_role.flink.arn
  application_mode       = "STREAMING"

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = aws_s3_bucket.lake.arn
          file_key   = aws_s3_object.flink_code.key
        }
      }
    }
    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval_ms
        min_pause_between_checkpoints = 30000
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.parallelism
        parallelism_per_kpu  = var.parallelism_per_kpu
        auto_scaling_enabled = false
      }
    }
    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = {
          STREAM_NAME          = aws_kinesis_stream.events.name
          AWS_REGION           = var.aws_region
          PARALLELISM          = tostring(var.parallelism)
          WINDOW_MINUTES       = tostring(var.window_minutes)
          OUT_OF_ORDER_SECONDS = tostring(var.out_of_order_seconds)
        }
      }
    }
  }

  cloudwatch_logging_options { log_stream_arn = aws_cloudwatch_log_stream.flink.arn }
  depends_on = [aws_iam_role_policy.flink]
}
