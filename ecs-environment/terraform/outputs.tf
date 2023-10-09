output "provisioner_lambda_url" {
  description = "The URL for the ECS provisioner lambda"
  value       = module.ecs_environment.provisioner_lambda_url
}
