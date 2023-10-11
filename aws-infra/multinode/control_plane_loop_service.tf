resource "aws_cloudwatch_log_group" "control_loop_logs" {
  name = "/${var.deployment_name}/control-loop"
}


resource "aws_ecs_task_definition" "control_loop" {
  family = "${var.deployment_name}-control-loop"

  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"

  execution_role_arn = aws_iam_role.execution_role.arn
  task_role_arn      = aws_iam_role.control_plane_task_role.arn

  cpu    = 512
  memory = 2048

  container_definitions = jsonencode([
    {
      name      = local.container_name
      image     = var.control_plane_docker_image
      essential = true
      entrypoint = [
        "loop"
      ]
      command = var.delete_db_tables_when_control_loop_terminates ? [
        "--provisioner=ecs",
        "--create-tables",
        "--delete-tables"
        ] : [
        "--provisioner=ecs",
        "--create-tables"
      ]
      environment = local.control_plane_environment_variables
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.control_loop_logs.name
          awslogs-region        = data.aws_region.current.name
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
}

resource "aws_ecs_service" "loop" {
  name            = "${var.deployment_name}-control-loop"
  cluster         = aws_ecs_cluster.cluster.name
  task_definition = aws_ecs_task_definition.control_loop.arn

  desired_count                      = 1
  deployment_maximum_percent         = 100
  deployment_minimum_healthy_percent = 0

  network_configuration {
    subnets          = var.task_subnet_ids
    security_groups  = [aws_security_group.control_plane_tasks.id]
    assign_public_ip = true
  }
}
