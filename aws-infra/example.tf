provider "aws" {
  # Set to the region you want to deploy in.
  region = "eu-west-2"
}

module "multinode_deployment" {
  source = "./multinode"

  # Required: A prefix used to generate names for all AWS resources created as part of this deployment.
  deployment_name = "multinode-test"

  # Required: The Docker image URI for the multinode control plane.
  control_plane_docker_image = "multinodedev/controlplane:1.0.1"

  # Required: The control plane API key. Can be anything you like.
  control_plane_api_key = "apikey"

  # Required: Networking
  # The load balancer subnets (used for the control plane load balancer) must be public.
  # The task subnets (used for the control plane processes and the workers) can be public or private.
  vpc_id = "vpc-08cd5773aede91ccf"
  load_balancer_subnet_ids = [
    "subnet-0f1abcc644e0041f6",
    "subnet-07350b22534e0f5f8",
    "subnet-040ebe53e62377f3e"
  ]
  task_subnet_ids = [
    "subnet-0de32a15fe52ef96b",
    "subnet-09cb75ee540dcf4f2",
    "subnet-05529f4d3c8f1511a"
  ]

  # Optional: Security group modifications
  # In this example, our VPC contains a service or data store whose security group is sg-0583d500dc1d94f2e.
  # We modify this security group so that the data store accepts traffic from our workers on port 5439.
  # ingress_from_worker_security_group = [
  #   {
  #     target_security_group_id : "sg-0583d500dc1d94f2e",
  #     ingress_port : 5439
  #   }
  # ]

  # Optional: IAM permissions
  # In this example, we attach an IAM policy to the workers, granting the workers full access to S3.
  # worker_task_role_policy_arns = [
  #   "arn:aws:iam::aws:policy/AmazonS3FullAccess"
  # ]

  # Optional: TLS and DNS (recommended for production)
  # In this example, we have Route53 hosted zone, which controls the domain quantumboost-development.com.
  # We create a DNS record for our control plane API, under the subdomain test.quantumboost-development.com,
  # and we create a TLS certificate for this subdomain.
  # use_tls_for_control_plane_api = true
  # hosted_zone_id                = "Z101297012ARZFQU7UT6O"
  # control_plane_api_subdomain   = "test.quantumboost-development.com"
}

output "control_plane_api_url" {
  description = "The URL for the control plane API"
  value       = module.multinode_deployment.control_plane_api_url
}
