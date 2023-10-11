# This task role grants permissions to a worker during code execution
# e.g. the worker may need permissions to read from an S3 bucket
resource "aws_iam_role" "worker_task_role" {
  name = "${var.deployment_name}-worker-task-role"

  assume_role_policy = jsonencode({
    "Version" : "2008-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "ecs-tasks.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "task_role_policy_attachment" {
  count      = length(var.worker_task_role_policy_arns)
  name       = "${var.deployment_name}-worker-task-policy-${count.index}"
  roles      = [aws_iam_role.worker_task_role.name]
  policy_arn = var.worker_task_role_policy_arns[count.index]
}
