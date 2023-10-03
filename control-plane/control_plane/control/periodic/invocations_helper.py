from typing import NamedTuple

from pydantic import BaseModel

from control_plane.types.datatypes import (
    InvocationInfo,
    FunctionInfo,
    WorkerStatus,
    ExecutionOutcome,
    FunctionStatus,
)


class RunningInvocationsClassification(BaseModel):
    # Ideally would use NamedTuple, but that's not possible because lists are not hashable in Python.
    invocations_to_terminate: list[InvocationInfo]
    invocations_to_create_executions_for: list[InvocationInfo]
    invocations_to_leave_untouched: list[InvocationInfo]


class FunctionId(NamedTuple):
    # Used as a key in hash maps
    # NB we must use NamedTuple, not BaseModel. NamedTuples are immutable, and their
    # __hash__ and __eq__ methods are implemented based on hashes and equality of their contents.
    project_name: str
    version_id: str
    function_name: str


def classify_running_invocations(
    running_invocations: list[InvocationInfo],
    functions_in_ready_status: list[FunctionInfo],
    time: int,
) -> RunningInvocationsClassification:
    # {function id: how many workers we're allowed to create for this function}
    initial_capacities = extract_initial_capacitites(functions_in_ready_status)
    remaining_capacities = subtract_running_invocations_from_capacities(initial_capacities, running_invocations)

    invocations_to_terminate: list[InvocationInfo] = []
    invocations_to_create_executions_for: list[InvocationInfo] = []
    invocations_to_leave_untouched: list[InvocationInfo] = []

    for invocation in running_invocations:
        function_id = FunctionId(
            project_name=invocation.project_name,
            version_id=invocation.version_id,
            function_name=invocation.function_name,
        )

        if function_id not in remaining_capacities.keys():
            # If function is not in ready status, we should leave the invocation as is
            invocations_to_leave_untouched.append(invocation)
        elif not all(execution.worker_status == WorkerStatus.TERMINATED for execution in invocation.executions):
            # If invocation has a non-terminated execution, we should leave it as is
            invocations_to_leave_untouched.append(invocation)
        elif any(
            execution.outcome in {ExecutionOutcome.SUCCEEDED, ExecutionOutcome.ABORTED}
            for execution in invocation.executions
        ):
            # If invocation has finished running successfully, we should terminate it
            invocations_to_terminate.append(invocation)
        elif (
            invocation.cancellation_requested
            or has_timed_out(invocation, time)
            or has_reached_retries_limit(invocation)
        ):
            # If invocation was cancelled, has timed out, or has reached retries limit;
            # we should terminate it
            invocations_to_terminate.append(invocation)
        elif remaining_capacities[function_id] >= 1:
            # If none of the above apply and the function has remaining capacity,
            # we should create a new execution.
            invocations_to_create_executions_for.append(invocation)
            remaining_capacities[function_id] -= 1
        else:
            # Otherwise, we should leave it as is
            invocations_to_leave_untouched.append(invocation)

    return RunningInvocationsClassification(
        invocations_to_terminate=invocations_to_terminate,
        invocations_to_create_executions_for=invocations_to_create_executions_for,
        invocations_to_leave_untouched=invocations_to_leave_untouched,
    )


def extract_initial_capacitites(
    functions_in_ready_status: list[FunctionInfo],
) -> dict[FunctionId, int]:
    capacities: dict[FunctionId, int] = dict()
    for function in functions_in_ready_status:
        function_id = FunctionId(
            project_name=function.project_name,
            version_id=function.version_id,
            function_name=function.function_name,
        )
        capacities[function_id] = function.resource_spec.max_concurrency

    return capacities


def subtract_running_invocations_from_capacities(
    capacities: dict[FunctionId, int], running_invocations: list[InvocationInfo]
) -> dict[FunctionId, int]:
    remaining_function_capacities = capacities.copy()
    for invocation in running_invocations:
        if any(execution.worker_status != WorkerStatus.TERMINATED for execution in invocation.executions):
            function_id = FunctionId(
                project_name=invocation.project_name,
                version_id=invocation.version_id,
                function_name=invocation.function_name,
            )
            assert function_id in remaining_function_capacities.keys()
            # This invocation is currently using the function's capacity.
            remaining_function_capacities[function_id] -= 1

    return remaining_function_capacities


def has_timed_out(invocation: InvocationInfo, time: int) -> bool:
    timeout_seconds = invocation.execution_spec.timeout_seconds
    time_elapsed = time - invocation.creation_time
    return time_elapsed > timeout_seconds


def has_reached_retries_limit(invocation: InvocationInfo) -> bool:
    # Convention in software engineering:
    # If max_retries = N, then you can run the function up to (N + 1) times.
    max_attempts = invocation.execution_spec.max_retries + 1
    num_attempts_so_far = len(invocation.executions)
    return num_attempts_so_far >= max_attempts
