output "provisioner_lambda_url" {
  description = "The URL for the ECS provisioner lambda"
  value       = aws_lambda_function_url.provisioner_lambda.function_url
}
