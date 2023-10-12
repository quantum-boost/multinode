from pathlib import Path

import click

from multinode.config import load_config_from_env
from multinode.constants import ROOT_WORKER_DIR
from multinode.utils.api import get_authenticated_client
from multinode.worker.runner import WorkerContext, WorkerRunner


@click.command()
def run_worker() -> None:
    config = load_config_from_env()
    api_client = get_authenticated_client(config)
    context = WorkerContext.from_env()
    runner = WorkerRunner(api_client, context, Path(ROOT_WORKER_DIR))
    runner.run_worker()
