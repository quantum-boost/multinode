from typing import NoReturn

import click


def cli_fail(ctx: click.Context, message: str) -> NoReturn:
    click.secho(message, fg="red", bold=True)
    ctx.exit(1)
