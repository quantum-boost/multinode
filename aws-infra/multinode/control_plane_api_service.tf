resource "aws_cloudwatch_log_group" "control_api_logs" {
  name = "/${var.deployment_name}/control-api"
}


resource "aws_ecs_task_definition" "control_api" {
  family = "${var.deployment_name}-control-api"

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
      portMappings = [
        {
          name          = "5000-tcp"
          containerPort = local.container_port
          hostPort      = local.host_port
          protocol      = "tcp"
          appProtocol   = "http"
        }
      ]
      entrypoint = [
        "api"
      ]
      command = [
        "--provisioner=ecs"
      ]
      environment = local.control_plane_environment_variables
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.control_api_logs.name
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

resource "aws_lb_target_group" "control_api" {
  name        = "${var.deployment_name}-control-api"
  port        = local.target_group_port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = var.vpc_id

  health_check {
    healthy_threshold = 2
    interval          = 300
  }
}

resource "aws_ecs_service" "control_api" {
  name            = "${var.deployment_name}-control-api"
  cluster         = aws_ecs_cluster.cluster.name
  task_definition = aws_ecs_task_definition.control_api.arn

  desired_count                      = 1
  deployment_maximum_percent         = 200
  deployment_minimum_healthy_percent = 100

  network_configuration {
    subnets          = var.task_subnet_ids
    security_groups  = [aws_security_group.control_plane_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.control_api.arn
    container_name   = local.container_name
    container_port   = local.container_port
  }
}

resource "aws_lb" "control_api" {
  name               = "${var.deployment_name}-control-api"
  internal           = false
  load_balancer_type = "application"

  subnets         = var.load_balancer_subnet_ids
  security_groups = [aws_security_group.control_plane_load_balancer.id]
}

resource "aws_lb_listener" "control_api" {
  load_balancer_arn = aws_lb.control_api.arn
  port              = local.listener_port
  protocol          = var.use_tls_for_control_plane_api ? "HTTPS" : "HTTP"
  ssl_policy        = var.use_tls_for_control_plane_api ? "ELBSecurityPolicy-2016-08" : null
  certificate_arn   = var.use_tls_for_control_plane_api ? aws_acm_certificate.cert[0].arn : null

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.control_api.arn
  }
}

# If adding TLS to the control plane load balancer,
# then we need to create a DNS record for the control plane load balancer...
resource "aws_route53_record" "alias_record" {
  count = var.use_tls_for_control_plane_api ? 1 : 0

  zone_id         = var.hosted_zone_id
  name            = var.control_plane_api_subdomain
  type            = "A"
  allow_overwrite = true

  alias {
    name                   = aws_lb.control_api.dns_name
    zone_id                = aws_lb.control_api.zone_id
    evaluate_target_health = false
  }
}

# ... and we also need to create the TLS certificate.
resource "aws_acm_certificate" "cert" {
  count = var.use_tls_for_control_plane_api ? 1 : 0

  domain_name       = var.control_plane_api_subdomain
  validation_method = "DNS"
}

resource "aws_route53_record" "cert_validation_record" {
  for_each = var.use_tls_for_control_plane_api ? {
    for dvo in aws_acm_certificate.cert[0].domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      record = dvo.resource_record_value
    }
  } : {}

  zone_id         = var.hosted_zone_id
  name            = each.value.name
  type            = each.value.type
  records         = [each.value.record]
  ttl             = 60
  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "cert_validation" {
  count = var.use_tls_for_control_plane_api ? 1 : 0

  certificate_arn         = aws_acm_certificate.cert[0].arn
  validation_record_fqdns = [for record in aws_route53_record.cert_validation_record : record.fqdn]
}
