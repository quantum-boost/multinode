from pathlib import Path

import click

from multinode.config import (
    create_control_plane_client_from_config,
    load_config_from_env,
)
from multinode.constants import ROOT_WORKER_DIR
from multinode.worker.runner import WorkerContext, WorkerRunner


@click.command()
def run_worker() -> None:
    config = load_config_from_env()
    api_client = create_control_plane_client_from_config(config)
    context = WorkerContext.from_env()
    runner = WorkerRunner(api_client, context, Path(ROOT_WORKER_DIR))
    runner.run_worker()
