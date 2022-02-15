resource "aws_secretsmanager_secret" "udacity_secret" {
  name = "udacity_dend2_secret"
}


resource "aws_secretsmanager_secret_version" "udacity_secret_version" {
  depends_on = [module.redshift]
  secret_id     = aws_secretsmanager_secret.udacity_secret.id
  secret_string = jsonencode(
    {
      REDSHIFT_END_POINT = module.redshift.redshift_cluster_endpoint
      RESHIFT_PASSWORD   = module.redshift.redshift_cluster_password
    })
}