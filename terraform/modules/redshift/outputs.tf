output "workgroup_endpoint" { value = aws_redshiftserverless_workgroup.analytics.endpoint }
output "admin_username" { value = var.redshift_admin_username }
output "iam_role_arn" { value = aws_iam_role.redshift.arn }

output "admin_password" {
  value     = random_password.redshift_admin.result
  sensitive = true
}
