locals {
  control_plane_api_url = (var.use_tls_for_control_plane_api ?
    "https://${tostring(var.control_plane_api_subdomain)}" :
    "http://${aws_lb.control_api.dns_name}"
  )

  control_plane_environment_variables = [
    {
      name  = "CONTROL_PLANE_API_KEY"
      value = var.control_plane_api_key
    },
    {
      name  = "POSTGRES_HOST"
      value = aws_rds_cluster_instance.control_plane.endpoint
    },
    {
      name  = "POSTGRES_DB"
      value = local.postgres_db
    },
    {
      name  = "POSTGRES_USER"
      value = local.postgres_user
    },
    {
      name  = "POSTGRES_PASSWORD"
      value = local.postgres_password
    },
    {
      name  = "CONTROL_PLANE_API_URL",
      value = local.control_plane_api_url
    },
    {
      name  = "AWS_REGION",
      value = data.aws_region.current.name
    },
    {
      name  = "CONTAINER_REPOSITORY_NAME",
      value = aws_ecr_repository.repo.repository_url
    },
    {
      name  = "CLUSTER_NAME",
      value = aws_ecs_cluster.cluster.name
    },
    {
      name  = "SUBNET_IDS",
      value = join(",", var.task_subnet_ids)
    },
    {
      name  = "SECURITY_GROUP_IDS"
      value = aws_security_group.worker_security_group.id
    },
    {
      name  = "TASK_ROLE_ARN",
      value = aws_iam_role.worker_task_role.arn
    },
    {
      name  = "EXECUTION_ROLE_ARN",
      value = aws_iam_role.execution_role.arn
    },
    {
      name  = "LOG_GROUP",
      value = aws_cloudwatch_log_group.worker_logs.name
    }
  ]

  container_name = "main"
}
