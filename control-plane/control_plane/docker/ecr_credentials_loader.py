import base64

import boto3
from botocore.config import Config

from control_plane.docker.credentials_loader import (
    AbstractContainerRepositoryCredentialsLoader,
)
from control_plane.types.datatypes import ContainerRepositoryCredentials


class EcrContainerRepositoryCredentialsLoader(
    AbstractContainerRepositoryCredentialsLoader
):
    def __init__(self, repository_name: str, aws_region: str):
        self._ecr_client = boto3.client("ecr", config=Config(region_name=aws_region))
        self._repository_name = repository_name

    def load(self) -> ContainerRepositoryCredentials:
        # It turns out that we don't need to pass in the repository name.
        # The credentials will be valid for all repositories that our IAM role has permissions for.
        response = self._ecr_client.get_authorization_token()

        auth_data = response["authorizationData"][0]

        token = base64.b64decode(auth_data["authorizationToken"]).decode()
        username, password = token.split(":")

        endpoint_url = auth_data["proxyEndpoint"]

        return ContainerRepositoryCredentials(
            repository_name=self._repository_name,
            username=username,
            password=password,
            endpoint_url=endpoint_url,
        )
