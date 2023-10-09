resource "aws_ecs_cluster" "cluster" {
  name = "${var.deployment_name}-cluster"
}

resource "aws_ecs_cluster_capacity_providers" "cluster" {
  cluster_name = aws_ecs_cluster.cluster.name

  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    base              = 0
    weight            = 1
    capacity_provider = "FARGATE"
  }
}

resource "aws_cloudwatch_log_group" "task_logs" {
  name = "/${var.deployment_name}"
}
