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
    # {function id: how many additional workers we're allowed to create for this function}
    remaining_function_capacities: dict[FunctionId, int] = dict()

    for function in functions_in_ready_status:
        function_id = FunctionId(
            project_name=function.project_name,
            version_id=function.version_id,
            function_name=function.function_name,
        )

        initial_capacity = function.resource_spec.max_concurrency
        remaining_function_capacities[function_id] = initial_capacity
        # Will subtract from this number later.

    for invocation in running_invocations:
        if any(execution.worker_status != WorkerStatus.TERMINATED for execution in invocation.executions):
            # This invocation is currently using the function's capacity.
            function_id = FunctionId(
                project_name=invocation.project_name,
                version_id=invocation.version_id,
                function_name=invocation.function_name,
            )
            assert function_id in remaining_function_capacities.keys()
            remaining_function_capacities[function_id] -= 1

    invocations_to_terminate: list[InvocationInfo] = []
    invocations_to_create_executions_for: list[InvocationInfo] = []
    invocations_to_leave_untouched: list[InvocationInfo] = []

    for invocation in running_invocations:
        function_id = FunctionId(
            project_name=invocation.project_name,
            version_id=invocation.version_id,
            function_name=invocation.function_name,
        )

        if all(execution.worker_status == WorkerStatus.TERMINATED for execution in invocation.executions):
            # This includes the very important sub-case where the invocation
            # does not yet have any executions at all.
            # This works because Python's all(...) returns True if the iterable is empty.

            if any(
                execution.outcome in {ExecutionOutcome.SUCCEEDED, ExecutionOutcome.ABORTED}
                for execution in invocation.executions
            ):
                invocations_to_terminate.append(invocation)

            else:
                cancellation_requested = invocation.cancellation_requested

                timeout_seconds = invocation.execution_spec.timeout_seconds
                time_elapsed = time - invocation.creation_time
                timed_out = time_elapsed > timeout_seconds

                # Convention in software engineering:
                # If max_retries = N, then you can run the function up to (N + 1) times.
                max_attempts = invocation.execution_spec.max_retries + 1
                num_attempts_so_far = len(invocation.executions)
                reached_retries_limit = num_attempts_so_far >= max_attempts

                if cancellation_requested or timed_out or reached_retries_limit:
                    invocations_to_terminate.append(invocation)

                else:
                    function_is_ready = invocation.function_status == FunctionStatus.READY
                    function_has_capacity = function_is_ready and remaining_function_capacities[function_id] >= 1

                    if function_has_capacity:
                        invocations_to_create_executions_for.append(invocation)
                        remaining_function_capacities[function_id] -= 1
                    else:
                        invocations_to_leave_untouched.append(invocation)

        else:
            invocations_to_leave_untouched.append(invocation)

    return RunningInvocationsClassification(
        invocations_to_terminate=invocations_to_terminate,
        invocations_to_create_executions_for=invocations_to_create_executions_for,
        invocations_to_leave_untouched=invocations_to_leave_untouched,
    )
