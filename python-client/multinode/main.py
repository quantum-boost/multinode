from getpass import getpass

import click

from multinode.api_client import ApiClient, Configuration, DefaultApi
from multinode.api_client.exceptions import ForbiddenException
from multinode.config import load_config, save_config

API_KEY_MISSING_MSG = click.style(
    "You are not logged in. Run `multinode login` first.", fg="red"
)

INVALID_API_KEY_MSG = click.style("API key is invalid.", fg="red")
SUCCESSFUL_LOGIN_MSG = click.style("You have successfully logged in!", fg="green")

SUCCESSFUL_LOGOUT_MSG = "You have successfully logged out."


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
        click.echo(API_KEY_MISSING_MSG)
        ctx.exit(code=1)


@cli.command()
@click.pass_context
def login(ctx: click.Context):
    multinode_config = load_config()
    if multinode_config.api_key is not None:
        click.echo(
            "You are already logged in. If you want to log in with "
            "a different account, run `multinode logout` first."
        )
        return

    # Current login flow just asks for API key directly. Alternatives include
    # redirecting to an authenticated web session or exchanging username/password for
    # an API key. Both require additional work and are not necessary for the MVP.
    api_key = getpass("Enter your API key:")
    multinode_config.api_key = api_key

    # Check if the API key is valid
    client_config = Configuration(host=multinode_config.api_url, access_token=api_key)
    client = ApiClient(client_config)
    api = DefaultApi(client)
    try:
        api.list_projects_projects_get()
    except ForbiddenException:
        click.echo(INVALID_API_KEY_MSG)
        ctx.exit(code=1)

    save_config(multinode_config)
    click.echo(SUCCESSFUL_LOGIN_MSG)


@cli.command()
def logout():
    config = load_config()
    config.api_key = None
    save_config(config)
    click.echo(SUCCESSFUL_LOGOUT_MSG)


@cli.command()
def deploy():
    raise NotImplementedError


@cli.command()
def undeploy():
    raise NotImplementedError


@cli.command()
def upgrade():
    raise NotImplementedError


@cli.command()
def list():
    raise NotImplementedError


@cli.command()
def describe():
    raise NotImplementedError


@cli.command()
def logs():
    raise NotImplementedError


if __name__ == "__main__":
    cli()
