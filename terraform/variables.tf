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
  default     = "../flink/target/flink-kinesis-processor.jar"
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
