resource "aws_redshift_cluster" "default" {
  cluster_identifier = var.cluster_identifier
  database_name      = var.db_name
  master_username    = var.username
  master_password    = random_password.password.result
  node_type          = var.node_type
  cluster_type       = var.cluster_type
  skip_final_snapshot = true
}

resource "random_password" "password" {
  length           = 16
  special          = true
  override_special = "_%@"
}
