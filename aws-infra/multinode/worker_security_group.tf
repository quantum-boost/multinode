resource "aws_security_group" "worker_security_group" {
  name        = "${var.deployment_name}-worker-sg"
  description = "Security group for worker tasks"
  vpc_id      = var.vpc_id

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

# The account may have other security groups, which need to allow ingress from the workers' security group.
# For example, there might be an RDS database that the workers need to access.
# In this example, the RDS database security group needs to allow ingress from the workers' security group.
resource "aws_security_group_rule" "ingress_from_worker_security_group" {
  count = length(var.ingress_from_worker_security_group)

  security_group_id        = var.ingress_from_worker_security_group[count.index].target_security_group_id
  type                     = "ingress"
  from_port                = var.ingress_from_worker_security_group[count.index].ingress_port
  to_port                  = var.ingress_from_worker_security_group[count.index].ingress_port
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.worker_security_group.id
}
