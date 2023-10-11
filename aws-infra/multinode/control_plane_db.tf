resource "random_string" "postgres_password" {
  length  = 16
  special = false
}

locals {
  postgres_db       = "postgres"
  postgres_user     = "postgres"
  postgres_password = random_string.postgres_password.result
}

resource "aws_rds_cluster" "control_plane" {
  cluster_identifier = "${var.deployment_name}-control-plane"
  engine             = "aurora-postgresql"
  engine_mode        = "provisioned"
  engine_version     = "15.3"

  database_name   = local.postgres_db
  master_username = local.postgres_user
  master_password = local.postgres_password

  vpc_security_group_ids = [aws_security_group.control_plane_database.id]
  storage_encrypted      = true

  skip_final_snapshot = true

  serverlessv2_scaling_configuration {
    max_capacity = 4.0
    min_capacity = 0.5
  }
}

resource "aws_rds_cluster_instance" "control_plane" {
  cluster_identifier = aws_rds_cluster.control_plane.id
  identifier         = "${var.deployment_name}-control-plane-writer"
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.control_plane.engine
  engine_version     = aws_rds_cluster.control_plane.engine_version
}
