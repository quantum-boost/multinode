data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# This task role grants permissions to the control plane during code execution.
# The permissions include the ability to create worker tasks.
resource "aws_iam_role" "control_plane_task_role" {
  name = "${var.deployment_name}-control-plane-task-role"

  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
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

  inline_policy {
    name = "ECSAccess"

    policy = jsonencode({
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "ecr:GetAuthorizationToken",
            "ecr:BatchCheckLayerAvailability",
            "ecr:PutImage",
            "ecr:InitiateLayerUpload",
            "ecr:UploadLayerPart",
            "ecr:CompleteLayerUpload"
          ],
          "Resource" : "*"
        },
        {
          "Effect" : "Allow",
          "Action" : [
            "ecs:RegisterTaskDefinition"
          ],
          "Resource" : "*"
        },
        {
          "Effect" : "Allow",
          "Action" : [
            "ecs:RunTask",
            "ecs:StopTask",
            "ecs:DescribeTasks"
          ],
          "Resource" : [
            "arn:aws:ecs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:task-definition/*",
            "arn:aws:ecs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:task/${aws_ecs_cluster.cluster.name}/*"
          ]
        },
        {
          "Effect" : "Allow",
          "Action" : "logs:GetLogEvents",
          "Resource" : "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:*:log-stream:*"
        },
        {
          "Effect" : "Allow",
          "Action" : "iam:PassRole",
          "Resource" : [
            aws_iam_role.execution_role.arn,
            aws_iam_role.worker_task_role.arn
          ]
        }
      ]
    })
  }
}
