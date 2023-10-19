from getpass import getpass
from pathlib import Path
from typing import List, Optional

import click

from multinode.api_client import ProjectInfo
from multinode.api_client.exceptions import ForbiddenException, NotFoundException
from multinode.cli.deployment import ProjectDeploymentOption, deploy_new_project_version
from multinode.cli.describe import (
    describe_function,
    describe_invocation,
    describe_project,
    describe_version,
)
from multinode.cli.fail import cli_fail
from multinode.config import (
    create_control_plane_client_from_config,
    load_config_from_file,
    save_config_to_file,
)
from multinode.constants import LATEST_VERSION_STR


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    # If no command is provided, or user tries to log in or log out,
    # proceed with click's default behavior
    if ctx.invoked_subcommand is None or ctx.invoked_subcommand in ["login", "logout"]:
        return

    # Otherwise, check if user is logged in
    config = load_config_from_file()
    if config.api_key is None:
        cli_fail(ctx, "You are not logged in. Run `multinode login` first.")


@cli.command()
@click.pass_context
def login(ctx: click.Context) -> None:
    config = load_config_from_file()
    if config.api_key is not None:
        click.echo(
            "You are already logged in. If you want to log in with "
            "a different account, run `multinode logout` first."
        )
        return

    # Current login flow just asks for API key directly. Alternatives include
    # redirecting to an authenticated web session or exchanging username/password for
    # an API key. Both require additional work and are not necessary for the MVP.
    api_key = getpass("Enter your API key:")
    config.api_key = api_key

    # Check if the API key is valid
    api_client = create_control_plane_client_from_config(config)
    try:
        api_client.list_projects()
    except ForbiddenException:
        cli_fail(ctx, "API key is invalid.")

    save_config_to_file(config)
    click.secho("You have successfully logged in!", fg="green")


@cli.command()
def logout() -> None:
    config = load_config_from_file()
    config.api_key = None
    save_config_to_file(config)
    click.echo("You have successfully logged out!")


@cli.command()
@click.argument(
    "project-dir",
    type=click.Path(path_type=Path, exists=True),
)
@click.option(
    "--project-name",
    type=str,
    required=True,
    help=(
        "Name that will be assigned to the deployed project. "
        "The command will fail if a project with the same name already exists."
    ),
)
@click.pass_context
def deploy(ctx: click.Context, project_dir: Path, project_name: str) -> None:
    """Deploy a Multinode project based on the code in FILEPATH."""
    deploy_new_project_version(
        ctx, project_dir, project_name, ProjectDeploymentOption.CREATE_NEW
    )


@cli.command()
@click.option(
    "--project-name",
    type=str,
    required=True,
)
@click.pass_context
def undeploy(ctx: click.Context, project_name: str) -> None:
    """Undeploy a Multinode project.

    In-flight functions will be cancelled before the project is deleted.
    """
    config = load_config_from_file()
    api_client = create_control_plane_client_from_config(config)
    try:
        api_client.delete_project(project_name)
    except NotFoundException:
        cli_fail(ctx, f'Project "{project_name}" does not exist.')

    click.secho(
        f'Project "{project_name}" has been successfully marked for deletion.',
        fg="green",
        bold=True,
    )


@cli.command()
@click.argument(
    "project-dir",
    type=click.Path(path_type=Path, exists=True),
)
@click.option(
    "--project-name",
    type=str,
    required=True,
    help="Name of the project that should be upgraded.",
)
@click.option(
    "--deploy",
    is_flag=True,
    default=False,
    help="If a project with this name does not exist, deploy it.",
)
@click.pass_context
def upgrade(
    ctx: click.Context, project_dir: Path, project_name: str, deploy: bool
) -> None:
    """Upgrade a Multinode project based on the code in PROJECT_DIR."""
    deployment_opt = (
        ProjectDeploymentOption.CREATE_IF_EXISTS_OTHERWISE_UPGRADE
        if deploy
        else ProjectDeploymentOption.UPGRADE_EXISTING
    )
    deploy_new_project_version(
        ctx,
        project_dir,
        project_name,
        deployment_opt,
    )


@cli.command()
def list() -> None:
    """List all deployed projects."""
    config = load_config_from_file()
    api_client = create_control_plane_client_from_config(config)
    projects: List[ProjectInfo] = api_client.list_projects().projects
    if len(projects) == 0:
        click.echo("You have not deployed any projects yet.")
        return

    click.echo("Deployed projects:")
    for p in projects:
        fg = "reset"
        deletion_suffix = ""
        if p.deletion_request_time is not None:
            deletion_suffix = " (marked for deletion)"
            fg = "red"

        click.secho(f"\t{p.project_name}{deletion_suffix}", fg=fg)


@cli.command()
@click.option("--project-name", type=str, required=True)
@click.option(
    "--version-id", type=str, help="If not provided, the latest version is used."
)
@click.option(
    "--function-name",
    type=str,
    help="If provided, detailed description of the function is returned.",
)
@click.option(
    "--invocation-id",
    type=str,
    help=(
        "Requires --function-name. "
        "If provided, detailed description of the invocation is returned."
    ),
)
@click.pass_context
def describe(
    ctx: click.Context,
    project_name: str,
    version_id: Optional[str],
    function_name: Optional[str],
    invocation_id: Optional[str],
) -> None:
    """Provides detailed description of a project, version, function, or invocation."""
    config = load_config_from_file()
    api_client = create_control_plane_client_from_config(config)
    resolved_version_id = version_id or LATEST_VERSION_STR

    # Project and version need to exist regardless of what the user wants to describe
    try:
        project = api_client.get_project(project_name)
    except NotFoundException:
        cli_fail(ctx, f'Project "{project_name}" does not exist.')

    try:
        version = api_client.get_project_version(
            project.project_name, resolved_version_id
        )
    except NotFoundException:
        cli_fail(
            ctx,
            f'Version "{resolved_version_id}" does not exist '
            f'on project "{project_name}".',
        )

    if function_name is None and invocation_id is None:
        if version_id is None:
            describe_project(api_client, project)

        describe_version(api_client, project, version, latest=version_id is None)
        return

    # Alert user that function/invocation is assumed to be for the latest version
    # if version_id is not provided
    if version_id is None:
        click.echo(
            f"Version id was not provided, "
            f"showing details for {version.version_id} (latest)\n"
        )

    # Function is needed for describing both function and invocation
    try:
        function = next(
            f for f in version.functions if f.function_name == function_name
        )
    except StopIteration:
        cli_fail(
            ctx,
            f'Function "{function_name}" does not exist '
            f'on version "{resolved_version_id}" of project "{project_name}".',
        )

    if invocation_id is None:
        describe_function(api_client, project, version, function)
        return

    try:
        invocation = api_client.get_invocation(
            project.project_name,
            version.version_id,
            function.function_name,
            invocation_id,
        )
    except NotFoundException:
        cli_fail(
            ctx,
            f'Invocation "{invocation_id}" does not exist '
            f'for function "{function_name}" '
            f'on version "{resolved_version_id}" '
            f'of project "{project_name}".',
        )

    describe_invocation(api_client, project, version, function, invocation)


@cli.command()
def logs() -> None:
    raise NotImplementedError


if __name__ == "__main__":
    cli()
