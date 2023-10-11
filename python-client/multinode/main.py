from getpass import getpass
from pathlib import Path
from typing import List

import click

from multinode.api_client import ProjectInfo
from multinode.api_client.exceptions import ForbiddenException, NotFoundException
from multinode.config import load_config, save_config
from multinode.utils.api import (
    create_project,
    create_project_version,
    get_authenticated_client,
)
from multinode.utils.errors import ProjectAlreadyExists
from multinode.utils.imports import import_multinode_object_from_file


@click.group()
@click.pass_context
def cli(ctx: click.Context):
    # If no command is provided, or user tries to log in or log out,
    # proceed with click's default behavior
    if ctx.invoked_subcommand is None or ctx.invoked_subcommand in ["login", "logout"]:
        return

    # Otherwise, check if user is logged in
    config = load_config()
    if config.api_key is None:
        click.secho("You are not logged in. Run `multinode login` first.", fg="red")
        ctx.exit(code=1)


@cli.command()
@click.pass_context
def login(ctx: click.Context):
    config = load_config()
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
    api_client = get_authenticated_client(config)
    try:
        api_client.list_projects()
    except ForbiddenException:
        click.secho("API key is invalid.", fg="red")
        ctx.exit(code=1)

    save_config(config)
    click.secho("You have successfully logged in!", fg="green")


@cli.command()
def logout():
    config = load_config()
    config.api_key = None
    save_config(config)
    click.echo("You have successfully logged out!")


@cli.command()
@click.argument("filepath", type=click.Path(exists=True))
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
def deploy(ctx: click.Context, filepath: Path, project_name: str):
    """Deploy a Multinode project based on the code in FILEPATH."""
    config = load_config()
    api_client = get_authenticated_client(config)
    try:
        multinode_obj = import_multinode_object_from_file(filepath)
    except ImportError as e:
        click.secho(e.msg, fg="red")
        ctx.exit(1)
        return  # for mypy

    try:
        project = create_project(api_client, project_name)
    except ProjectAlreadyExists:
        click.secho(
            f'Project "{project_name}" already exists. '
            f"Please choose a different name or run `multinode upgrade` instead.",
            fg="red",
        )
        ctx.exit(1)
        return  # for mypy

    version = create_project_version(api_client, project.project_name, multinode_obj)
    click.secho(
        f'Project "{version.project_name}" has been successfully deployed! '
        f"Version id = {version.version_id}",
        fg="green",
    )


@cli.command()
@click.option(
    "--project-name",
    type=str,
    required=True,
)
@click.pass_context
def undeploy(ctx: click.Context, project_name: str):
    """Undeploy a Multinode project.

    In-flight functions will be cancelled before the project is deleted.
    """
    config = load_config()
    api_client = get_authenticated_client(config)
    try:
        api_client.delete_project(project_name)
    except NotFoundException:
        click.secho(f'Project "{project_name}" does not exist.', fg="red")
        ctx.exit(1)
        return  # for mypy

    click.secho(
        f'Project "{project_name}" has been successfully marked for deletion.',
        fg="green",
    )


@cli.command()
@click.argument("filepath", type=click.Path(exists=True))
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
def upgrade(ctx: click.Context, filepath: Path, project_name: str, deploy: bool):
    """Upgrade a Multinode project based on the code in FILEPATH."""
    config = load_config()
    api_client = get_authenticated_client(config)
    try:
        multinode_obj = import_multinode_object_from_file(filepath)
    except ImportError as e:
        click.secho(e.msg, fg="red")
        ctx.exit(1)
        return  # for mypy

    if deploy:
        try:
            project = create_project(api_client, project_name)
            project_name = project.project_name
            click.echo(f'Project "{project_name}" does not exist. Deploying...')
        except ProjectAlreadyExists:
            click.echo(f'Project "{project_name}" already exists. Upgrading...')

    try:
        version = create_project_version(api_client, project_name, multinode_obj)
    except NotFoundException:
        click.secho(f'Project "{project_name}" does not exist. ', fg="red")
        ctx.exit(1)
        return  # for mypy

    click.secho(
        f'Project "{version.project_name}" has been successfully upgraded! '
        f"New version id = {version.version_id}",
        fg="green",
    )


@cli.command()
def list():
    """List all deployed projects."""
    config = load_config()
    api_client = get_authenticated_client(config)
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
def describe():
    raise NotImplementedError


@cli.command()
def logs():
    raise NotImplementedError


if __name__ == "__main__":
    cli()
