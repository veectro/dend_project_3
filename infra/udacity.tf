module "redshift" {
  source = "./modules/redshift"
  cluster_identifier = "udacity-redshift-cluster"
  username = "dwhuser"
  node_type = "dc2.large"
  db_name = "dwh"
  iam_roles = [aws_iam_role.redshift_iam_role.arn]
}
