from typing import NamedTuple

from control_plane.types.datatypes import ExecutionInfo


class RunningExecutionsClassification(NamedTuple):
    executions_requiring_termination_signal: list[ExecutionInfo]
    executions_to_leave_untouched: list[ExecutionInfo]


def classify_running_executions(
    running_executions: list[ExecutionInfo], time: int
) -> RunningExecutionsClassification:
    executions_requiring_termination_signal: list[ExecutionInfo] = []
    executions_to_leave_untouched: list[ExecutionInfo] = []

    for execution in running_executions:
        if not execution.termination_signal_sent:
            cancellation_requested = execution.cancellation_requested

            timeout_seconds = execution.execution_spec.timeout_seconds
            time_elapsed = time - execution.invocation_creation_time
            timed_out = time_elapsed > timeout_seconds

            if cancellation_requested or timed_out:
                executions_requiring_termination_signal.append(execution)
            else:
                executions_to_leave_untouched.append(execution)

        else:
            # If the termination signal has already been sent, then there is no need to send it again.
            executions_to_leave_untouched.append(execution)

    return RunningExecutionsClassification(
        executions_requiring_termination_signal=executions_requiring_termination_signal,
        executions_to_leave_untouched=executions_to_leave_untouched,
    )
