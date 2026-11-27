# Ingesta: stream de Kinesis cifrado y copia cruda a S3 vía Firehose.

resource "aws_kinesis_stream" "events" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_hours
  encryption_type  = "KMS"
  kms_key_id       = var.kms_key_arn
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.prefix}-to-s3"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_iam_role" "firehose" {
  name = "${var.prefix}-firehose-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "firehose.amazonaws.com" },
    Action = "sts:AssumeRole", Condition = { StringEquals = { "sts:ExternalId" = var.account_id } } }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  name = "${var.prefix}-firehose-policy"
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["kinesis:DescribeStream", "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards"], Resource = aws_kinesis_stream.events.arn },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = var.kms_key_arn },
      { Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"], Resource = [var.bucket_arn, "${var.bucket_arn}/streaming/raw/*"] },
      { Effect = "Allow", Action = ["logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.firehose.arn}:*" }
    ]
  })
}

resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = "${var.prefix}-to-s3"
  destination = "extended_s3"
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events.arn
    role_arn           = aws_iam_role.firehose.arn
  }
  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose.arn
    bucket_arn         = var.bucket_arn
    prefix             = "streaming/raw/"
    buffering_size     = 5
    buffering_interval = 60
    compression_format = "UNCOMPRESSED"
    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }
  depends_on = [aws_iam_role_policy.firehose]
}
