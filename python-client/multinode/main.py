import click


@click.group()
def cli():
    pass


@cli.command()
def login():
    raise NotImplementedError


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
