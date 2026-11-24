terraform {
  backend "s3" {
    key            = "aws-realtime-flink-pipeline/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "tfstate-locks"
    encrypt        = true
    # bucket se pasa con: terraform init -backend-config=backend.hcl
  }
}
