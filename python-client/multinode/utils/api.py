from typing import Any, Callable, Dict, List

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
from multinode.api_client.error_types import resolve_error
from multinode.config import Config

Func = Callable[[Any], Any]


def convert_exceptions(method: Func) -> Func:
    def method_with_exceptions_converted(
        *args: List[Any], **kwargs: Dict[str, Any]
    ) -> Any:
        try:
            return method(*args, **kwargs)
        except ApiException as e:
            raise resolve_error(e)

    return method_with_exceptions_converted


def convert_exceptions_on_all_methods(cls: type) -> type:
    for name in dir(cls):
        if not name.startswith("__"):  # Exclude special methods
            attribute = getattr(cls, name)
            if callable(attribute):
                setattr(cls, name, convert_exceptions(attribute))
    return cls


@convert_exceptions_on_all_methods
class Api(DefaultApi):
    pass


def get_authenticated_client(multinode_config: Config) -> Api:
    client_config = Configuration(
        host=multinode_config.api_url, access_token=multinode_config.api_key
    )
    client = ApiClient(client_config)
    return Api(client)


def create_project(api_client: Api, project_name: str) -> ProjectInfo:
    return api_client.create_project(project_name)


def create_project_version(
    api_client: Api,
    project_name: str,
    multinode_obj: Multinode,
) -> VersionInfo:
    functions = [job.fn_spec for job in multinode_obj.jobs.values()]
    # TODO nginx is just a placeholder, provide actual docker image
    version_def = VersionDefinition(
        default_docker_image="nginx:latest", functions=functions
    )
    version = api_client.create_project_version(
        project_name=project_name, version_definition=version_def
    )
    return version
