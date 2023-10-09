variable "deployment_name" {
  description = "A prefix used in names of all deployed resources"
  type        = string
}

variable "provisioner_api_key" {
  description = "The API key for the ECS provisioner"
  type        = string
}

variable "control_plane_api_url" {
  description = "The URL for the control plane API"
  type        = string
}

variable "control_plane_api_key" {
  description = "The API key for the control plane"
  type        = string
}

variable "task_role_policy_arns" {
  description = "The ARNs of the IAM policies to attach to the ECS task role"
  type        = list(string)
}

variable "vpc_id" {
  description = "The ID of the VPC to run the ECS tasks in"
  type        = string
}

variable "subnet_ids" {
  description = "The IDs of the VPC subnets to run the ECS tasks in"
  type        = list(string)
}

variable "subnets_are_public" {
  description = "Whether the VPC subnets are public"
  type        = bool
}

variable "ingress_rules_from_task_security_group" {
  description = "A list of security groups that should allow ingress from the security group attached to the ECS tasks, together with the ports on which ingress is allowed"
  type = list(object({
    target_security_group_id = string
    ingress_port             = number
  }))
}
