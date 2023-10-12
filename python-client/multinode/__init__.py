import math
from datetime import timedelta
from typing import Any, Callable, Dict, Optional, TypeVar

from multinode.api_client import ExecutionSpec, FunctionSpec, ResourceSpec
from multinode.core.job import Job

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class Multinode:
    def __init__(self) -> None:
        self.jobs: Dict[str, Job[Any, Any]] = {}

    def job(
        self,
        docker_image: Optional[str] = None,
        max_retries: int = 0,
        timeout: Optional[timedelta] = None,
        cpu: float = 0.1,
        memory: str = "100 MiB",
        max_concurrency: int = 10,
    ) -> Callable[[Callable[[InputT], OutputT]], Job[InputT, OutputT]]:
        """
        Decorator that transforms a Python function into a Multinode `Job`.

        :param docker_image: Docker image to use when running the job. If not provided,
            project's default image will be used.
        :param max_retries: How many times to retry the job if it fails (either due
            to an exception or a hardware failure).
        :param timeout: timeout to use for the job. If the job doesn't finish before the
            timeout, the `TimeoutError` will be raised inside its process.
            Defaults to 1 hour.
        :param cpu: The amount of virtual CPUs to allocate for the job.
        :param memory: The amount of memory to allocate for the job.
        :param max_concurrency: How many instances of the job can run at the same time.
            If the limit is reached, new jobs will be queued until there is spare
            capacity.
        """
        if timeout is None:
            timeout = timedelta(hours=1)

        resource_spec = ResourceSpec(
            virtual_cpus=cpu,
            memory_gbs=_memory_str_to_gb(memory),
            max_concurrency=max_concurrency,
        )
        exec_spec = ExecutionSpec(
            max_retries=max_retries,
            timeout_seconds=math.ceil(timeout.total_seconds()),
        )

        def decorator(fn: Callable[[InputT], OutputT]) -> Job[InputT, OutputT]:
            fn_spec = FunctionSpec(
                function_name=fn.__name__,
                docker_image_override=docker_image,
                resource_spec=resource_spec,
                execution_spec=exec_spec,
            )
            job_obj: Job[InputT, OutputT] = Job(fn_spec)

            self.jobs[fn.__name__] = job_obj
            return job_obj

        return decorator


def _memory_str_to_gb(memory: str) -> float:
    if memory.endswith("GiB"):
        return float(memory[:-3])
    elif memory.endswith("MiB"):
        return float(memory[:-3]) / 1024
    elif memory.endswith("KiB"):
        return float(memory[:-3]) / 1024 / 1024
    else:
        raise ValueError(
            f"Invalid memory string '{memory}'. "
            f"It should end with GiB, MiB, or KiB. Examples: '100 MiB' or '2 GiB'"
        )
