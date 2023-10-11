This terraform deploys multinode into your AWS account, using ECS to provision workers.

**Your AWS account** must contain:
- _The VPC in which multinode will run._ (Usually the same VPC as the rest of your application.)
- (Optional) _A Route53 hosted zone_ - necessary if you want to add TLS and DNS to the multinode control plane API. 
- (Optional) _IAM policies for the workers_ - necessary if your code will call AWS services.

**Software requirements:**
- [The AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

**Step 1.** Get an [access key](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey) for your AWS account.

**Step 2.** Configure your AWS CLI to use your access key.
```commandline
aws configure
```
... then enter your access key when prompted.

**Step 3.** Edit `example.tf` according to how your AWS account is set up.

**Step 4.** Deploy multinode using the terraform.
```commandline
terraform init
terraform apply
```

**Step 5.** Take note of the `control_plane_api_url` outputted by the terraform.