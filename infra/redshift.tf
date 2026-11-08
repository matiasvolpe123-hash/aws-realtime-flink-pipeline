# ------------------------------------------------------------------------------
# Capa de analítica de baja latencia: Redshift Serverless lee directamente del
# stream de Kinesis (Streaming Ingestion, Módulo 2) y consulta las tablas
# Iceberg del lakehouse (Módulo 4) vía Redshift Spectrum sobre el mismo
# Glue Data Catalog. No se usa un clúster provisionado clásico: Serverless
# evita pagar por capacidad ociosa mientras se desarrolla el checkpoint.
# ------------------------------------------------------------------------------

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
  description = "CIDR permitido para conectarse al puerto 5439 del workgroup (tu IP pública /32, por ejemplo). No se deja abierto a 0.0.0.0/0."
  type        = string
}

# Subredes y VPC: se reutiliza la VPC por defecto de la cuenta/región para no
# duplicar una red completa solo para este checkpoint. En un entorno de
# producción, Redshift iría en subredes privadas de una VPC dedicada.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "redshift" {
  name        = "${local.prefix}-redshift-sg"
  description = "Acceso al workgroup de Redshift Serverless (puerto 5439)."
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Cliente SQL (psql, DBeaver, etc.)"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.redshift_allowed_cidr]
  }

  egress {
    description = "Salida necesaria para leer Kinesis y Glue/S3"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Password del admin: se genera, no se hardcodea. Queda solo en el state
# (por eso el backend remoto de Terraform debe estar cifrado) y se expone
# como output sensible para recuperarlo una única vez.
resource "random_password" "redshift_admin" {
  length  = 20
  special = true
  # Redshift no acepta algunos caracteres especiales en la password del admin.
  override_special = "!#$%^&*()-_=+"
}

# ------------------------------------------------------------------------------
# Rol de IAM del workgroup: permisos de Streaming Ingestion sobre Kinesis y de
# lectura del Glue Data Catalog / objetos Iceberg en S3 (Redshift Spectrum).
# Es un rol propio, separado del rol de ejecución de Flink (flink.tf): Flink
# escribe, Redshift solo lee.
# ------------------------------------------------------------------------------
resource "aws_iam_role" "redshift" {
  name = "${local.prefix}-redshift-analytics-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

data "aws_iam_policy_document" "redshift_analytics" {
  statement {
    sid = "KinesisStreamingIngestion"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.events.arn]
  }

  statement {
    sid       = "KinesisListStreams"
    actions   = ["kinesis:ListStreams"]
    resources = ["*"]
  }

  statement {
    sid       = "KinesisStreamKms"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.stream.arn]
  }

  statement {
    sid = "GlueCatalogRead"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.lakehouse_db.arn,
      "${aws_glue_catalog_database.lakehouse_db.arn}/*",
    ]
  }

  statement {
    sid       = "LakehouseDataRead"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/warehouse/*"]
  }

  statement {
    sid       = "LakehouseBucketList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
}

resource "aws_iam_role_policy" "redshift_analytics" {
  name   = "${local.prefix}-redshift-analytics-policy"
  role   = aws_iam_role.redshift.id
  policy = data.aws_iam_policy_document.redshift_analytics.json
}

resource "aws_redshiftserverless_namespace" "analytics" {
  namespace_name       = "${local.prefix}-analytics"
  db_name              = "analytics"
  admin_username       = var.redshift_admin_username
  admin_user_password  = random_password.redshift_admin.result
  iam_roles            = [aws_iam_role.redshift.arn]
  default_iam_role_arn = aws_iam_role.redshift.arn
  log_exports          = ["userlog", "connectionlog", "useractivitylog"]
}

resource "aws_redshiftserverless_workgroup" "analytics" {
  namespace_name      = aws_redshiftserverless_namespace.analytics.namespace_name
  workgroup_name      = "${local.prefix}-analytics-wg"
  base_capacity       = var.redshift_base_capacity_rpu
  subnet_ids          = data.aws_subnets.default.ids
  security_group_ids  = [aws_security_group.redshift.id]
  publicly_accessible = true
}

output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.analytics.endpoint
}

output "redshift_admin_username" {
  value = var.redshift_admin_username
}

output "redshift_admin_password" {
  value     = random_password.redshift_admin.result
  sensitive = true
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift.arn
}
