resource "aws_security_group" "task_security_group" {
  name        = "${var.deployment_name}-task-sg"
  description = "Security group for ECS task"
  vpc_id      = var.vpc_id

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_security_group_rule" "ingress_from_task_security_group" {
  count = length(var.ingress_rules_from_task_security_group)

  security_group_id        = var.ingress_rules_from_task_security_group[count.index].target_security_group_id
  type                     = "ingress"
  from_port                = var.ingress_rules_from_task_security_group[count.index].ingress_port
  to_port                  = var.ingress_rules_from_task_security_group[count.index].ingress_port
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.task_security_group.id
}
