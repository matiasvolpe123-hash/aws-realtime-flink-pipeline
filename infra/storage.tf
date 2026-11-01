resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = true
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
