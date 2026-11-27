variable "prefix" {
  description = "Prefijo de nombres (proyecto-entorno)."
  type        = string
}

variable "bucket_name" {
  description = "Nombre del bucket del data lake."
  type        = string
}

variable "stream_name" {
  description = "Nombre del stream de Kinesis (solo para la descripción de la clave KMS)."
  type        = string
}
