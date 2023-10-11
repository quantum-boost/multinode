variable "deployment_name" {
  description = "A prefix used in names of all deployed resources"
  type        = string
}

variable "control_plane_docker_image" {
  description = "The URI of the docker image for the control plane"
  type        = string
}

variable "control_plane_api_key" {
  description = "The API key for the control plane"
  type        = string
}

variable "vpc_id" {
  description = "The ID of the VPC in which to deploy the ECS tasks and control plane load balancer"
  type        = string
}

variable "task_subnet_ids" {
  description = "The IDs of the VPC subnets to run the ECS tasks in"
  type        = list(string)
}

variable "load_balancer_subnet_ids" {
  description = "The IDs of the VPC subnets to provision the control plane load balancer in (must be public subnets)"
  type        = list(string)
}

variable "ingress_from_worker_security_group" {
  description = "A list of security groups that should allow ingress from the security group attached to the worker tasks, together with the ports on which this ingress is allowed"
  type = list(object({
    target_security_group_id = string
    ingress_port             = number
  }))
  default = []
}

variable "worker_task_role_policy_arns" {
  description = "The ARNs of the IAM policies to attach to the ECS task role"
  type        = list(string)
  default     = []
}

variable "use_tls_for_control_plane_api" {
  description = "Whether the control plane load balancer should accept HTTPS traffic"
  type        = bool
  default     = false
}

variable "hosted_zone_id" {
  description = "The ID of the Route53 hosted zone that will provide DNS for the control plane API. Required if use_tls_for_control_plane_api is true"
  type        = string
  default     = null
}

variable "control_plane_api_subdomain" {
  description = "The subdomain name for the control plane API (without the https:// at the beginning). Must be a subdomain controlled by the hosted zone. Required if use_tls_for_control_plane_api is true"
  type        = string
  default     = null
}

variable "delete_db_tables_when_control_loop_terminates" {
  description = "Whether to delete database tables when the control loop process terminates. Should usually be set to false, except in development scenarios."
  type        = bool
  default     = false
}
