variable "aws_region" {
  description = "AWS region for this environment."
  type        = string
  default     = "us-east-2"
}

variable "project_name" {
  type    = string
  default = "realtime-sensors"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "artifact_path" {
  description = "Path to the shaded Flink JAR, relative to the terraform directory."
  type        = string
  default     = "../flink-app/target/flink-kinesis-processor.jar"
}

variable "shard_count" {
  type    = number
  default = 2
}

variable "parallelism" {
  type    = number
  default = 2
}

variable "parallelism_per_kpu" {
  type    = number
  default = 1
}

variable "window_minutes" {
  type    = number
  default = 1
}

variable "out_of_order_seconds" {
  type    = number
  default = 10
}

variable "checkpoint_interval_ms" {
  type    = number
  default = 60000
}

variable "retention_hours" {
  description = "Retención del stream de Kinesis en horas (24 = mínimo, sin cargo extra)."
  type        = number
  default     = 24
}

variable "redshift_base_capacity_rpu" {
  description = "Capacidad base del workgroup, en Redshift Processing Units (mínimo 8)."
  type        = number
  default     = 8
}

variable "redshift_admin_username" {
  description = "Usuario administrador del namespace de Redshift."
  type        = string
  default     = "admin_analytics"
}

variable "redshift_allowed_cidr" {
  description = "CIDR permitido para conectarse al puerto 5439 del workgroup (tu IP pública /32). No se deja abierto a 0.0.0.0/0."
  type        = string
}
