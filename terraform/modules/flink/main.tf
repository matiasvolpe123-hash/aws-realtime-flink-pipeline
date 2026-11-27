# Procesamiento: aplicación de Managed Service for Apache Flink, su rol IAM y logs.

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.application_name}"
  retention_in_days = 14
}

resource "aws_s3_object" "flink_code" {
  bucket       = var.bucket_id
  key          = var.code_key
  source       = var.artifact_path
  source_hash  = filebase64sha256(var.artifact_path)
  content_type = "application/java-archive"
}

resource "aws_iam_role" "flink" {
  name = "${var.prefix}-flink-execution-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "kinesisanalytics.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "flink" {
  name = "${var.prefix}-flink-runtime-policy"
  role = aws_iam_role.flink.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards"], Resource = var.stream_arn },
      { Effect = "Allow", Action = ["kinesis:ListStreams"], Resource = "*" },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = var.kms_key_arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = aws_s3_object.flink_code.arn },
      { Effect = "Allow", Action = ["glue:GetDatabase", "glue:GetDatabases", "glue:CreateTable", "glue:GetTable", "glue:GetTables", "glue:UpdateTable", "glue:DeleteTable", "glue:GetTableVersion", "glue:GetTableVersions"], Resource = ["arn:aws:glue:${var.aws_region}:${var.account_id}:catalog", var.glue_database_arn, "arn:aws:glue:${var.aws_region}:${var.account_id}:table/${var.glue_database_name}/*"] },
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = var.bucket_arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], Resource = "${var.bucket_arn}/warehouse/*" },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:DescribeLogStreams", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.flink.arn}:*" }
    ]
  })
}

resource "aws_kinesisanalyticsv2_application" "flink" {
  name                   = var.application_name
  runtime_environment    = "FLINK-1_18"
  service_execution_role = aws_iam_role.flink.arn
  application_mode       = "STREAMING"
  start_application      = true # arranca el job al desplegar, sin paso manual

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = var.bucket_arn
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
    run_configuration {
      application_restore_configuration {
        application_restore_type = "SKIP_RESTORE" # primer arranque: no hay snapshot previo
      }
    }
    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = {
          STREAM_NAME            = var.stream_name
          AWS_REGION             = var.aws_region
          PARALLELISM            = tostring(var.parallelism)
          WINDOW_MINUTES         = tostring(var.window_minutes)
          OUT_OF_ORDER_SECONDS   = tostring(var.out_of_order_seconds)
          CHECKPOINT_INTERVAL_MS = tostring(var.checkpoint_interval_ms)
          GLUE_DATABASE          = var.glue_database_name
          LAKEHOUSE_WAREHOUSE    = "s3://${var.bucket_name}/warehouse"
          ICEBERG_TABLE          = "sensor_window_summaries"
        }
      }
    }
  }

  cloudwatch_logging_options { log_stream_arn = aws_cloudwatch_log_stream.flink.arn }
  depends_on = [aws_iam_role_policy.flink]
}
