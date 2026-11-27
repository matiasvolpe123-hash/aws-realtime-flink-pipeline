variable "prefix" {
  type = string
}

variable "stream_name" {
  type = string
}

variable "shard_count" {
  type = number
}

variable "retention_hours" {
  description = "Retención del stream en horas. 24 es el mínimo y no tiene cargo extra."
  type        = number
}

variable "kms_key_arn" {
  type = string
}

variable "bucket_arn" {
  type = string
}

variable "account_id" {
  type = string
}
