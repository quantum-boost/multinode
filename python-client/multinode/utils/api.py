from multinode import Multinode
from multinode.api_client import (
    ApiClient,
    ApiException,
    Configuration,
    DefaultApi,
    VersionDefinition,
    VersionInfo,
)
from multinode.config import Config
from multinode.utils.errors import ProjectAlreadyExists


def get_authenticated_client(multinode_config: Config) -> DefaultApi:
    client_config = Configuration(
        host=multinode_config.api_url, access_token=multinode_config.api_key
    )
    client = ApiClient(client_config)
    return DefaultApi(client)


def deploy_multinode(
    api_client: DefaultApi,
    project_name: str,
    multinode_obj: Multinode,
) -> VersionInfo:
    """
    Deploys project, version, and functions to the Multinode API.

    :raises ProjectAlreadyExists: if the with given `project_name` project already exists.
    """
    try:
        project = api_client.create_project(project_name)
    except ApiException as e:
        if e.status == 409:
            raise ProjectAlreadyExists
        else:
            raise e

    functions = [job.fn_spec for job in multinode_obj.jobs.values()]
    # TODO nginx is just a placeholder, provide actual docker image
    version_def = VersionDefinition(
        default_docker_image="nginx:latest", functions=functions
    )
    version = api_client.create_project_version(
        project_name=project.project_name, version_definition=version_def
    )
    return version
