data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  lambda_name = "${var.deployment_name}-provisioner"
}

resource "aws_iam_role" "provisioner_lambda_role" {
  name = "${local.lambda_name}-role"

  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })

  inline_policy {
    name = "LogsAccess"

    policy = jsonencode({
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : "logs:CreateLogGroup",
          "Resource" : "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
        },
        {
          "Effect" : "Allow",
          "Action" : [
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          "Resource" : [
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.lambda_name}:*"
          ]
        }
      ]
    })
  }

  inline_policy {
    name = "ECSAccess"

    policy = jsonencode({
      "Version" : "2012-10-17",
      "Statement" : [
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
            "arn:aws:ecs:eu-west-2:921216064263:task-definition/*",
            "arn:aws:ecs:eu-west-2:921216064263:task/${aws_ecs_cluster.cluster.name}/*"
          ]
        },
        {
          "Effect" : "Allow",
          "Action" : "logs:GetLogEvents",
          "Resource" : "arn:aws:logs:eu-west-2:921216064263:log-group:*:log-stream:*"
        },
        {
          "Effect" : "Allow",
          "Action" : "iam:PassRole",
          "Resource" : [
            aws_iam_role.task_execution_role.arn,
            aws_iam_role.task_role.arn
          ]
        }
      ]
    })
  }
}

resource "aws_lambda_function" "provisioner_lambda" {
  function_name = local.lambda_name
  handler       = "main.lambda_handler"
  role          = aws_iam_role.provisioner_lambda_role.arn
  runtime       = "python3.11"

  s3_bucket = "multinode-public-assets"
  s3_key    = "provisioner.zip"

  environment {
    variables = {
      PROVISIONER_API_KEY   = var.provisioner_api_key
      CONTROL_PLANE_API_URL = var.control_plane_api_url
      CONTROL_PLANE_API_KEY = var.control_plane_api_key
      CLUSTER_NAME          = aws_ecs_cluster.cluster.name
      SUBNET_IDS            = join(",", var.subnet_ids)
      ASSIGN_PUBLIC_IP      = tostring(var.subnets_are_public)
      SECURITY_GROUP_IDS    = aws_security_group.task_security_group.id
      TASK_ROLE_ARN         = aws_iam_role.task_role.arn
      EXECUTION_ROLE_ARN    = aws_iam_role.task_execution_role.arn
      LOG_GROUP             = aws_cloudwatch_log_group.task_logs.name
    }
  }

  timeout = 30
}

resource "aws_lambda_function_url" "provisioner_lambda" {
  function_name      = aws_lambda_function.provisioner_lambda.function_name
  authorization_type = "NONE"
}
