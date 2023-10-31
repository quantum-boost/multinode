# Deployment instructions

This terraform deploys multinode into your AWS account, using ECS to provision workers.

**Your AWS account** must contain:

- _The VPC in which multinode will run._ (Usually the same VPC as the rest of your application. Or if you just want to try out Multinode, then it is fine to use the default VPC.)
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

**Step 3.** Clone the repo, then edit `example.tf` according to how your AWS account is set up.

**Step 4.** Deploy multinode using the terraform.

```commandline
cd ./aws-infra/
terraform init
terraform apply
```

**Step 5.** Take note of the `control_plane_api_url` outputted by the terraform. Also take note of the `control_plane_api_key` in the `example.tf`, which you will need later.

**To undeploy**:

```commandline
terraform destroy
```

### Recommendation: Increasing Fargate quotas

If you have never previously used Fargate in your AWS account, then we strongly recommend that you
[increase your account quota](https://docs.aws.amazon.com/servicequotas/latest/userguide/request-quota-increase.html).

- Service name: AWS Fargate
- Quota name: Fargate On-Demand vCPU resource count

By default, the quota is only 6 vCPUs, which doesn't allow you to take full advantage of Multinode.
We suggest increasing this to at least 64 vCPUs.

While waiting for the quota increase, you can still proceed with the documentation and examples in this repo
to get a feel for how Multinode works.
