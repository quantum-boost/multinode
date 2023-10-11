resource "aws_cloudwatch_log_group" "worker_logs" {
  name = "/${var.deployment_name}/workers"
}
