output "bucket_id" { value = aws_s3_bucket.lake.id }
output "bucket_name" { value = aws_s3_bucket.lake.bucket }
output "bucket_arn" { value = aws_s3_bucket.lake.arn }
output "kms_key_arn" { value = aws_kms_key.stream.arn }
output "glue_database_name" { value = aws_glue_catalog_database.lakehouse_db.name }
output "glue_database_arn" { value = aws_glue_catalog_database.lakehouse_db.arn }
