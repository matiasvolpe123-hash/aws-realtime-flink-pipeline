# Stack completo dividido en cuatro módulos (ver ./modules):
#   storage   -> bucket S3, clave KMS y base Glue
#   ingestion -> stream de Kinesis y Firehose a S3
#   flink     -> aplicación de Managed Flink
#   redshift  -> Redshift Serverless (Streaming Ingestion + Spectrum)

module "storage" {
  source = "./modules/storage"

  prefix      = local.prefix
  bucket_name = local.bucket_name
  stream_name = local.stream_name
}

module "ingestion" {
  source = "./modules/ingestion"

  prefix          = local.prefix
  stream_name     = local.stream_name
  shard_count     = var.shard_count
  retention_hours = var.retention_hours
  kms_key_arn     = module.storage.kms_key_arn
  bucket_arn      = module.storage.bucket_arn
  account_id      = data.aws_caller_identity.current.account_id
}

module "flink" {
  source = "./modules/flink"

  prefix           = local.prefix
  application_name = local.application_name
  code_key         = local.code_key
  aws_region       = var.aws_region
  account_id       = data.aws_caller_identity.current.account_id
  artifact_path    = var.artifact_path

  stream_name        = module.ingestion.stream_name
  stream_arn         = module.ingestion.stream_arn
  kms_key_arn        = module.storage.kms_key_arn
  bucket_id          = module.storage.bucket_id
  bucket_arn         = module.storage.bucket_arn
  bucket_name        = module.storage.bucket_name
  glue_database_name = module.storage.glue_database_name
  glue_database_arn  = module.storage.glue_database_arn

  parallelism            = var.parallelism
  parallelism_per_kpu    = var.parallelism_per_kpu
  window_minutes         = var.window_minutes
  out_of_order_seconds   = var.out_of_order_seconds
  checkpoint_interval_ms = var.checkpoint_interval_ms
}

module "redshift" {
  source = "./modules/redshift"

  prefix     = local.prefix
  aws_region = var.aws_region
  account_id = data.aws_caller_identity.current.account_id

  stream_arn         = module.ingestion.stream_arn
  kms_key_arn        = module.storage.kms_key_arn
  bucket_arn         = module.storage.bucket_arn
  glue_database_name = module.storage.glue_database_name
  glue_database_arn  = module.storage.glue_database_arn

  redshift_base_capacity_rpu = var.redshift_base_capacity_rpu
  redshift_admin_username    = var.redshift_admin_username
  redshift_allowed_cidr      = var.redshift_allowed_cidr
}
