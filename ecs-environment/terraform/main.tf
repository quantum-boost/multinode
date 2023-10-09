provider "aws" {
  region = "eu-west-2"
}

terraform {
  backend "s3" {
    bucket = "multinode-terraform-state"
    key    = "workers.tfstate"
    region = "eu-west-2"
  }
}

module "ecs_environment" {
  source                                 = "./ecs_environment"
  deployment_name                        = var.deployment_name
  task_role_policy_arns                  = var.task_role_policy_arns
  vpc_id                                 = var.vpc_id
  subnet_ids                             = var.subnet_ids
  subnets_are_public                     = var.subnets_are_public
  ingress_rules_from_task_security_group = var.ingress_rules_from_task_security_group
  provisioner_api_key                    = var.provisioner_api_key
  control_plane_api_key                  = var.control_plane_api_key
  control_plane_api_url                  = var.control_plane_api_url
}
