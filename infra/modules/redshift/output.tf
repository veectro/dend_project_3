output "redshift_cluster_endpoint" {
  value = aws_redshift_cluster.default.endpoint
}

output "redshift_cluster_password" {
  value = aws_redshift_cluster.default.master_password
}