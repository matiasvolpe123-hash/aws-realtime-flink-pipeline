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
      { Effect = "Allow", Action = ["glue:GetDatabase", "glue:GetDatabases", "glue:CreateTable", "glue:GetTable", "glue:GetTables", "glue:UpdateTable", "glue:DeleteTable", "glue:GetTableVersion", "glue:GetTableVersions"], Resource = ["arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog", aws_glue_catalog_database.lakehouse_db.arn, "${aws_glue_catalog_database.lakehouse_db.arn}/*"] },
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = aws_s3_bucket.lake.arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], Resource = "${aws_s3_bucket.lake.arn}/warehouse/*" },
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
          STREAM_NAME            = aws_kinesis_stream.events.name
          AWS_REGION             = var.aws_region
          PARALLELISM            = tostring(var.parallelism)
          WINDOW_MINUTES         = tostring(var.window_minutes)
          OUT_OF_ORDER_SECONDS   = tostring(var.out_of_order_seconds)
          CHECKPOINT_INTERVAL_MS = tostring(var.checkpoint_interval_ms)
          GLUE_DATABASE          = aws_glue_catalog_database.lakehouse_db.name
          LAKEHOUSE_WAREHOUSE    = "s3://${aws_s3_bucket.lake.bucket}/warehouse"
          ICEBERG_TABLE          = "sensor_window_summaries"
        }
      }
    }
  }

  cloudwatch_logging_options { log_stream_arn = aws_cloudwatch_log_stream.flink.arn }
  depends_on = [aws_iam_role_policy.flink]
}
