deployment_name = "multinode-workers"

task_role_policy_arns = [
  "arn:aws:iam::aws:policy/AmazonS3FullAccess"
]

vpc_id = "vpc-08cd5773aede91ccf"

subnet_ids = [
  "subnet-0f1abcc644e0041f6",
  "subnet-07350b22534e0f5f8",
  "subnet-040ebe53e62377f3e"
]

subnets_are_public = true

ingress_rules_from_task_security_group = [
  {
    target_security_group_id : "sg-0583d500dc1d94f2e",
    ingress_port : 5432
  }
]

provisioner_api_key = "lemonandherb"

control_plane_api_url = "https://control.quantumboost-development.com/"

control_plane_api_key = "butterflyburger"
