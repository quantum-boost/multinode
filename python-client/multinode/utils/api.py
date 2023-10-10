from multinode import Multinode
from multinode.api_client import (
    ApiClient,
    ApiException,
    Configuration,
    DefaultApi,
    ProjectInfo,
    VersionDefinition,
    VersionInfo,
)
from multinode.api_client.exceptions import NotFoundException
from multinode.config import Config
from multinode.utils.errors import ProjectAlreadyExists, ProjectDoesNotExist


def get_authenticated_client(multinode_config: Config) -> DefaultApi:
    client_config = Configuration(
        host=multinode_config.api_url, access_token=multinode_config.api_key
    )
    client = ApiClient(client_config)
    return DefaultApi(client)


def create_project(api_client: DefaultApi, project_name: str) -> ProjectInfo:
    try:
        project = api_client.create_project(project_name)
    except ApiException as e:
        if e.status == 409:
            raise ProjectAlreadyExists
        else:
            raise e

    return project


def create_project_version(
    api_client: DefaultApi,
    project_name: str,
    multinode_obj: Multinode,
) -> VersionInfo:
    functions = [job.fn_spec for job in multinode_obj.jobs.values()]
    # TODO nginx is just a placeholder, provide actual docker image
    version_def = VersionDefinition(
        default_docker_image="nginx:latest", functions=functions
    )
    try:
        version = api_client.create_project_version(
            project_name=project_name, version_definition=version_def
        )
    except NotFoundException:
        raise ProjectDoesNotExist

    return version
