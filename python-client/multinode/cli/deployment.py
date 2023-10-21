import shutil
import time
from enum import Enum
from pathlib import Path
from typing import Dict, Generator
from uuid import uuid4

import click
import docker
from docker.errors import BuildError, ImageNotFound

from multinode.api_client import (
    ContainerRepositoryCredentials,
    DefaultApi,
    ProjectInfo,
    VersionDefinition,
    VersionInfo,
)
from multinode.api_client.exceptions import ApiException, NotFoundException
from multinode.cli.fail import cli_fail
from multinode.config import (
    create_control_plane_client_from_config,
    load_config_from_file,
)
from multinode.constants import ROOT_WORKER_DIR
from multinode.core.errors import ProjectAlreadyExists
from multinode.core.multinode import Multinode
from multinode.utils.dynamic_imports import import_multinode_object_from_dir

# TODO we should allow user specify preferred Python version
DOCKER_TEMPLATE = f"""FROM python:3.8-bullseye

WORKDIR {ROOT_WORKER_DIR}
COPY . {ROOT_WORKER_DIR}

ENV PYTHONBUFFERED=1 \\
    PYTHONPATH={ROOT_WORKER_DIR} \\
    {{user_env_vars}}


{{install_req_line}}
RUN pip install multinode

ENTRYPOINT ["start-multinode-worker"]
"""

INSTALL_REQUIREMENTS_LINE = "RUN pip install -r requirements.txt"


class ProjectDeploymentOption(Enum):
    CREATE_NEW = "CREATE_NEW"
    UPGRADE_EXISTING = "UPGRADE_EXISTING"
    CREATE_IF_EXISTS_OTHERWISE_UPGRADE = "CREATE_IF_EXISTS_OTHERWISE_UPGRADE"


def deploy_new_project_version(
    ctx: click.Context,
    project_dir: Path,
    project_name: str,
    project_deployment_option: ProjectDeploymentOption,
) -> None:
    """Deploys new version of the project to Multinode cloud.

    All the packaging assumes PYTHONPATH to be at the provided directory, together
    with the main.py and optional requirements.txt and .env files.

    E.g., if user passes path to a "qb-suggestions" directory, the expectation is that
    it looks something like this:
    ```
        qb-suggestions/
        |-- qb_suggestions/
        |   |-- __init__.py
        |   |-- maths.py
        |   |-- suggestions.py
        |   |-- data_processing.py
        |-- main.py
        |-- requirements.txt
        |-- .env
    ```

    With examples of absolute imports being:
    ```
    from qb_suggestions.maths import nanming
    from qb_suggestions.data_processing import standardize
    # ... etc

    # or if anything was imported from `main.py` (although `main.py` should rather
    # import stuff from other files, not the other way around)
    from main import run_suggestions
    ```
    """
    config = load_config_from_file()
    api_client = create_control_plane_client_from_config(config)

    # Building and pushing the image takes a significant amount of time.
    # So we first need to check if the project deployment option won't be violated
    # since that's something we can check quickly.
    # At the same time we don't want to create the project yet because the image
    # build/push may fail leaving us with an empty project.
    try:
        api_client.get_project(project_name)
        if project_deployment_option == ProjectDeploymentOption.CREATE_NEW:
            cli_fail(
                ctx,
                f'Project "{project_name}" already exists. '
                f"Please choose a different name or run `multinode upgrade` instead.",
            )
    except NotFoundException:
        if project_deployment_option == ProjectDeploymentOption.UPGRADE_EXISTING:
            cli_fail(ctx, f'Project "{project_name}" does not exist.')

    multinode_obj = import_multinode_object_from_dir(project_dir)
    click.secho(f"Found Multinode object in {project_dir}.", fg="green", bold=True)

    container_repo_creds = api_client.get_container_repository_credentials()
    image_tag = _package_and_push_project_image(
        ctx, container_repo_creds, project_dir, project_name
    )

    if project_deployment_option != ProjectDeploymentOption.UPGRADE_EXISTING:
        # Even though we checked for existence of the project above, it may have been
        # created in the meantime by another user while image was being built/pushed.
        try:
            _create_project(api_client, project_name)
        except ProjectAlreadyExists:
            if project_deployment_option == ProjectDeploymentOption.CREATE_NEW:
                cli_fail(
                    ctx,
                    f'Project "{project_name}" already exists. '
                    f"Please choose a different name or run `multinode upgrade` instead.",
                )

    try:
        version = _create_project_version(
            api_client, project_name, multinode_obj, image_tag
        )
    except NotFoundException:
        cli_fail(ctx, f'Project "{project_name}" does not exist.')

    click.secho(
        f'Project "{version.project_name}" has been successfully deployed! '
        f"Version id = {version.version_id}",
        fg="green",
        bold=True,
    )


def _package_and_push_project_image(
    ctx: click.Context,
    repository_credentials: ContainerRepositoryCredentials,
    project_dir: Path,
    project_name: str,
) -> str:
    image_tag = f"{repository_credentials.repository_name}:{project_name}-{uuid4()}"

    main_filepath = project_dir / "main.py"
    if not main_filepath.exists():
        cli_fail(ctx, f"Could not find main.py file in the {project_dir} directory.")

    install_req_line = _get_install_requirements_line(project_dir)
    user_env_vars = _get_user_env_vars(ctx, project_dir)
    dockerfile = DOCKER_TEMPLATE.format(
        install_req_line=install_req_line, user_env_vars=user_env_vars
    )

    # Append current seconds to the directory name to ensure uniqueness
    time_seconds = int(time.time())
    temp_dist_dir = project_dir / f"multinode-dist-{time_seconds}"
    temp_dist_dir.mkdir()

    docker_client = docker.from_env()
    dockerfile_path = temp_dist_dir / "Dockerfile"
    relative_dockerfile_path = dockerfile_path.relative_to(project_dir)
    try:
        with dockerfile_path.open("w") as f:
            f.write(dockerfile)

        click.secho("\nBuilding Docker image...", bold=True)
        try:
            image, build_log = docker_client.images.build(
                path=str(project_dir),
                dockerfile=str(relative_dockerfile_path),
                tag=image_tag,
            )
            _pretty_print_docker_build_log(build_log)
        except BuildError as e:
            _pretty_print_docker_build_log(e.build_log)
            cli_fail(ctx, e.msg)

        click.secho("Successfully built Docker image!", fg="green", bold=True)
        click.secho("\nPushing image to the Multinode's repository...", bold=True)
        docker_client.login(
            username=repository_credentials.username,
            password=repository_credentials.password,
            registry=repository_credentials.endpoint_url,
        )
        push_log = docker_client.images.push(image_tag, stream=True, decode=True)
        _pretty_print_docker_push_log(push_log)
        click.secho(
            "Successfully pushed image to the Multinode's repository!",
            fg="green",
            bold=True,
        )
    finally:
        if temp_dist_dir.exists():
            shutil.rmtree(temp_dist_dir)

        try:
            docker_client.images.remove(image=image_tag)
        except ImageNotFound:
            pass  # `finally` block could have been executed before image was built

    return image_tag


def _get_install_requirements_line(project_dir: Path) -> str:
    requirements_path = project_dir / "requirements.txt"
    if not requirements_path.exists():
        click.secho(
            "We didn't detect the requirements.txt file in your project. "
            "It will run without any external dependencies.",
            fg="yellow",
        )
        return ""

    return INSTALL_REQUIREMENTS_LINE


def _get_user_env_vars(ctx: click.Context, project_dir: Path) -> str:
    env_path = project_dir / ".env"
    if not env_path.exists():
        click.secho(
            "We didn't detect the .env file in your project. "
            "It will run without any environment variables.",
            fg="yellow",
        )

    env_lines = []
    with open(env_path, "r") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                key, value = line.split("=", 1)
            except ValueError:
                cli_fail(
                    ctx,
                    f"Invalid environment variable definition "
                    f"on the line {i+1} of the .env file: {line}",
                )

            env_lines.append(f"{key.strip()}={value.strip()}")

    return " \\\n    ".join(env_lines)


def _pretty_print_docker_build_log(
    build_log: Generator[Dict[str, str], None, None]
) -> None:
    for line in build_log:
        if "stream" in line:
            click.echo(line["stream"], nl=False)


def _pretty_print_docker_push_log(
    push_log: Generator[Dict[str, str], None, None]
) -> None:
    layers_progress: Dict[str, str] = {}
    for line in push_log:
        if "id" not in line or "progress" not in line:
            continue

        # Move up in the terminal to clean up all existing progress bars
        for _ in layers_progress:
            click.echo("\033[F", nl=False)

        # Print new progress bars
        layers_progress[line["id"]] = line["progress"]
        for layer_id, progress in layers_progress.items():
            click.echo(f"\033[K{layer_id}: {progress}")  # Clean line and print progress


def _create_project(api_client: DefaultApi, project_name: str) -> ProjectInfo:
    try:
        project = api_client.create_project(project_name)
    except ApiException as e:
        if e.status == 409:
            raise ProjectAlreadyExists
        else:
            raise e

    return project


def _create_project_version(
    api_client: DefaultApi, project_name: str, multinode_obj: Multinode, image_tag: str
) -> VersionInfo:
    functions = [function.fn_spec for function in multinode_obj._functions.values()]
    version_def = VersionDefinition(default_docker_image=image_tag, functions=functions)
    version = api_client.create_project_version(
        project_name=project_name, version_definition=version_def
    )
    return version
