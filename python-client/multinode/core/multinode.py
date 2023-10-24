import math
from datetime import timedelta
from typing import Any, Callable, Dict

from multinode.api_client import ExecutionSpec, FunctionSpec, ResourceSpec
from multinode.core.function import Function
from multinode.errors import ParameterValidationError
from multinode.shared.parameter_bounds import (
    FUNCTION_NAME_LENGTH_LIMIT,
    MAX_CONCURRENCY_LIMIT,
    MAX_RETRIES_LIMIT,
    MEMORY_GBS_LIMIT,
    TIMEOUT_SECONDS_LIMIT,
    VIRTUAL_CPUS_LIMIT,
)

DEFAULT_TIMEOUT = timedelta(hours=1)


class Multinode:
    def __init__(self) -> None:
        self._functions: Dict[str, Function] = {}

    def function(
        self,
        max_retries: int = 0,
        timeout: timedelta = DEFAULT_TIMEOUT,
        cpu: float = 0.1,
        memory: str = "256 MiB",
        max_concurrency: int = 10,
    ) -> Callable[[Callable[..., Any]], Function]:
        """
        Decorator that transforms a Python function into a Multinode `Function`.

        :param max_retries: How many times to retry the function if it fails (either due
            to an exception or a hardware failure).
        :param timeout: timeout to use for the function. If the function doesn't finish
            before the timeout, an `InvocationTimedOutError` will be raised inside its process.
        :param cpu: The number of virtual CPUs to allocate for the function.
        :param memory: The amount of memory to allocate for the function.
            Examples: '100 MiB' or '2 GiB'.
        :param max_concurrency: How many instances of the function can run at the same
            time. If the limit is reached, new invocations will be queued until there
            is spare capacity.
        """
        _validate_max_retries(max_retries)
        _validate_cpu(cpu)
        _validate_max_concurrency(max_concurrency)

        resource_spec = ResourceSpec(
            virtual_cpus=cpu,
            memory_gbs=_memory_str_to_gb(memory),
            max_concurrency=max_concurrency,
        )
        exec_spec = ExecutionSpec(
            max_retries=max_retries,
            timeout_seconds=_timeout_to_seconds(timeout),
        )

        def decorator(fn: Callable[..., Any]) -> Function:
            _validate_function_name(fn.__name__)

            fn_spec = FunctionSpec(
                function_name=fn.__name__,
                resource_spec=resource_spec,
                execution_spec=exec_spec,
            )
            multinode_fn = Function(fn_spec, fn=fn)

            self._functions[fn.__name__] = multinode_fn
            return multinode_fn

        return decorator


def _memory_str_to_gb(memory: Any) -> float:
    if not isinstance(memory, str):
        raise ParameterValidationError(
            "memory must be a string. Examples: '100 MiB' or '2 GiB'"
        )

    if memory.endswith("GiB"):
        gbs_per_unit = 1
    elif memory.endswith("MiB"):
        gbs_per_unit = 1024
    elif memory.endswith("KiB"):
        gbs_per_unit = 1024 * 1024
    else:
        raise ParameterValidationError(
            "memory is incorrectly formatted. Expected format: '100 MiB' or '2 GiB'"
        )

    memory_in_original_units_str = memory[:-3].strip()
    try:
        memory_in_original_units = float(memory_in_original_units_str)
    except ValueError:
        raise ParameterValidationError(
            "memory is incorrectly formatted. Expected format: '100 MiB' or '2 GiB'"
        )

    memory_gbs = memory_in_original_units / gbs_per_unit

    if memory_gbs <= 0:
        raise ParameterValidationError("memory must be positive")
    if memory_gbs > MEMORY_GBS_LIMIT:
        raise ParameterValidationError(f"memory must not exceed {MEMORY_GBS_LIMIT} GiB")

    return memory_gbs


def _timeout_to_seconds(timeout: Any) -> int:
    if not isinstance(timeout, timedelta):
        raise ParameterValidationError("timeout must be a timedelta object")

    timeout_seconds = math.ceil(timeout.total_seconds())

    if timeout_seconds <= 0:
        raise ParameterValidationError("timeout must be positive")
    if timeout_seconds > TIMEOUT_SECONDS_LIMIT:
        raise ParameterValidationError(
            f"timeout must not exceed {TIMEOUT_SECONDS_LIMIT} seconds"
        )
    return timeout_seconds


def _validate_cpu(cpu: Any) -> None:
    _validate_bounded_number("cpu", cpu, expect_positive=True, limit=VIRTUAL_CPUS_LIMIT)


def _validate_max_retries(max_retries: Any) -> None:
    _validate_bounded_number(
        "max_retries", max_retries, expect_non_negative=True, limit=MAX_RETRIES_LIMIT
    )


def _validate_max_concurrency(max_concurrency: Any) -> None:
    _validate_bounded_number(
        "max_concurrency",
        max_concurrency,
        expect_positive=True,
        limit=MAX_CONCURRENCY_LIMIT,
    )


def _validate_bounded_number(
    name: str,
    value: Any,
    limit: int,
    expect_positive: bool = False,
    expect_non_negative: bool = False,
) -> None:
    if not (isinstance(value, int) or isinstance(value, float)):
        raise ParameterValidationError(f"{name} must be a number")
    if expect_positive and value <= 0:
        raise ParameterValidationError(f"{name} must be positive")
    if expect_non_negative and value < 0:
        raise ParameterValidationError(f"{name} must be non-negative")
    if value > limit:
        raise ParameterValidationError(f"{name} must not exceed {limit}")


def _validate_function_name(fn_name: str) -> None:
    if len(fn_name) > FUNCTION_NAME_LENGTH_LIMIT:
        raise ParameterValidationError(
            "function names must not exceed " f"{FUNCTION_NAME_LENGTH_LIMIT} characters"
        )
