# Data lake: bucket S3 privado y versionado, clave KMS del stream y base Glue.

resource "aws_s3_bucket" "lake" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
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
  description             = "KMS key for ${var.stream_name}"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "stream" {
  name          = "alias/${var.prefix}-stream"
  target_key_id = aws_kms_key.stream.key_id
}

resource "aws_glue_catalog_database" "lakehouse_db" {
  name         = "lakehouse_db"
  description  = "Iceberg tables produced by the real-time Flink pipeline"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/warehouse/"
}
