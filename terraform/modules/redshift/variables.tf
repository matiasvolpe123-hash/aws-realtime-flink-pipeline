variable "prefix" { type = string }
variable "aws_region" { type = string }
variable "account_id" { type = string }

variable "stream_arn" { type = string }
variable "kms_key_arn" { type = string }
variable "bucket_arn" { type = string }
variable "glue_database_name" { type = string }
variable "glue_database_arn" { type = string }

variable "redshift_base_capacity_rpu" {
  description = "Capacidad base del workgroup, en Redshift Processing Units (mínimo 8)."
  type        = number
}

variable "redshift_admin_username" {
  description = "Usuario administrador del namespace de Redshift."
  type        = string
}

variable "redshift_allowed_cidr" {
  description = "CIDR permitido para conectarse al puerto 5439 (tu IP pública /32). No se deja abierto a 0.0.0.0/0."
  type        = string
}
