locals {
  postgres_port     = 5432
  container_port    = 5000
  host_port         = 5000
  target_group_port = 80
  listener_port     = var.use_tls_for_control_plane_api ? 443 : 80
}

resource "aws_security_group" "control_plane_load_balancer" {
  name        = "${var.deployment_name}-control-plane-alb-sg"
  description = "Security group for the control plane load balancer"
  vpc_id      = var.vpc_id

  ingress {
    from_port        = local.listener_port
    to_port          = local.listener_port
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_security_group" "control_plane_tasks" {
  name        = "${var.deployment_name}-control-plane-task-sg"
  description = "Security group for the control plane tasks"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = local.container_port
    to_port         = local.container_port
    protocol        = "tcp"
    security_groups = [aws_security_group.control_plane_load_balancer.id]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_security_group" "control_plane_database" {
  name        = "${var.deployment_name}-control-plane-db-sg"
  description = "Security group for the control plane RDS database"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = local.postgres_port
    to_port         = local.postgres_port
    protocol        = "tcp"
    security_groups = [aws_security_group.control_plane_tasks.id]
  }
}