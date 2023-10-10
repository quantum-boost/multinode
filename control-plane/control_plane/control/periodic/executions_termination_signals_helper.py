from typing import NamedTuple

from control_plane.types.datatypes import ExecutionInfo


class RunningExecutionsClassification(NamedTuple):
    executions_requiring_termination_signal: list[ExecutionInfo]
    executions_to_leave_untouched: list[ExecutionInfo]


def classify_running_executions_for_termination_signals(
    running_executions: list[ExecutionInfo], time: int
) -> RunningExecutionsClassification:
    executions_requiring_termination_signal: list[ExecutionInfo] = []
    executions_to_leave_untouched: list[ExecutionInfo] = []

    for execution in running_executions:
        if execution.termination_signal_sent:
            # If the termination signal has already been sent, then there is no need to send it again.
            executions_to_leave_untouched.append(execution)
        elif execution.cancellation_requested or _has_timed_out(execution, time):
            # Cancellation requested or timed out => send termination signal
            executions_requiring_termination_signal.append(execution)
        else:
            # Default: leave untouched
            executions_to_leave_untouched.append(execution)

    return RunningExecutionsClassification(
        executions_requiring_termination_signal=executions_requiring_termination_signal,
        executions_to_leave_untouched=executions_to_leave_untouched,
    )


def _has_timed_out(execution: ExecutionInfo, time: int) -> bool:
    timeout_seconds = execution.execution_spec.timeout_seconds
    time_elapsed = time - execution.invocation_creation_time
    return time_elapsed > timeout_seconds
