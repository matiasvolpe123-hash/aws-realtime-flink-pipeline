variable "prefix" { type = string }
variable "application_name" { type = string }
variable "code_key" { type = string }
variable "aws_region" { type = string }
variable "account_id" { type = string }

variable "artifact_path" {
  description = "Ruta al JAR de Flink, relativa al directorio donde se ejecuta terraform."
  type        = string
}

variable "stream_name" { type = string }
variable "stream_arn" { type = string }
variable "kms_key_arn" { type = string }
variable "bucket_id" { type = string }
variable "bucket_arn" { type = string }
variable "bucket_name" { type = string }
variable "glue_database_name" { type = string }
variable "glue_database_arn" { type = string }

variable "parallelism" { type = number }
variable "parallelism_per_kpu" { type = number }
variable "window_minutes" { type = number }
variable "out_of_order_seconds" { type = number }
variable "checkpoint_interval_ms" { type = number }
