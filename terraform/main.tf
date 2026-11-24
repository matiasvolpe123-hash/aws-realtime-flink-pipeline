resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_glue_catalog_database" "lakehouse_db" {
  name         = "lakehouse_db"
  description  = "Iceberg tables produced by the real-time Flink pipeline"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/warehouse/"
}
