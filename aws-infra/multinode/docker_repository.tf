resource "aws_ecr_repository" "repo" {
  name                 = "${var.deployment_name}-repo"
  image_tag_mutability = "IMMUTABLE"

  force_delete = true
}