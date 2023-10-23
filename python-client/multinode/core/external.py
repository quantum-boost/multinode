from typing import Optional

from multinode.api_client import FunctionSpec
from multinode.config import load_config_with_api_key_from_env_or_file
from multinode.constants import LATEST_VERSION_STR
from multinode.core.function import Function
from multinode.errors import FunctionDoesNotExist
from multinode.utils.api import get_authenticated_client


def get_deployed_function(
    project_name: str, function_name: str, version_id: Optional[str] = None
) -> Function:
    """It allows non-Multinode code to interact with deployed Multinode functions."""
    config = load_config_with_api_key_from_env_or_file()
    version_id = version_id or LATEST_VERSION_STR
    api_client = get_authenticated_client(config)
    version = api_client.get_project_version(project_name, version_id)

    try:
        f_info = next(f for f in version.functions if f.function_name == function_name)
    except StopIteration:
        raise FunctionDoesNotExist(
            f'Function "{function_name}" does not exist '
            f'on version "{version_id}" of project "{project_name}".'
        )

    fn_spec = FunctionSpec(
        function_name=f_info.function_name,
        docker_image_override=f_info.docker_image,
        resource_spec=f_info.resource_spec,
        execution_spec=f_info.execution_spec,
    )
    return Function(
        fn_spec, project_name=version.project_name, version_id=version.version_id
    )
