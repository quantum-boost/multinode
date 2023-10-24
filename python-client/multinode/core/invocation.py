from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, cast

import jsonpickle

from multinode.api_client import ExecutionOutcome, ExecutionSummary, InvocationInfo
from multinode.api_client import InvocationStatus as ApiInvocationStatus
from multinode.api_client import WorkerStatus


class StrEnum(str, Enum):
    def __repr__(self) -> str:
        return str.__repr__(self.value)

    def __str__(self) -> str:
        return str.__str__(self.value)


class InvocationStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"
    CANCELLING = "CANCELLING"
    TIMED_OUT = "TIMED OUT"
    RUNNING = "RUNNING"
    PENDING = "PENDING"
    FAILED = "FAILED"

    @property
    def finished(self) -> bool:
        return self in {
            InvocationStatus.SUCCEEDED,
            InvocationStatus.CANCELLED,
            InvocationStatus.TIMED_OUT,
            InvocationStatus.FAILED,
        }


@dataclass
class Invocation:
    status: InvocationStatus
    result: Optional[Any]
    error: Optional[str]
    terminated: bool
    num_failed_attempts: int

    @staticmethod
    def from_invocation_info(inv_info: InvocationInfo) -> Invocation:
        num_failed_attempts = _get_num_failed_executions(inv_info)
        status = _resolve_invocation_status(inv_info, num_failed_attempts)
        terminated = inv_info.invocation_status == ApiInvocationStatus.TERMINATED
        result = _extract_result(inv_info)
        error = _extract_error(inv_info)
        return Invocation(
            status=status,
            result=result,
            error=error,
            terminated=terminated,
            num_failed_attempts=num_failed_attempts,
        )

    def readable_status(self) -> str:
        clauses: list[str] = [self.status.value]

        if self.status.finished and not self.terminated:
            clauses.append(" - terminating")

        if self.num_failed_attempts > 0:
            plural_suffix = "s" if self.num_failed_attempts != 1 else ""
            if self.status == InvocationStatus.FAILED:
                clauses.append(
                    f", {self.num_failed_attempts} failed attempt{plural_suffix}"
                )
            elif self.status in {InvocationStatus.PENDING, InvocationStatus.RUNNING}:
                clauses.append(
                    f", {self.num_failed_attempts} previous failed attempt{plural_suffix}"
                )

        return "(" + "".join(clauses) + ")"


def _resolve_invocation_status(
    inv_info: InvocationInfo, num_failed_attempts: int
) -> InvocationStatus:
    if _has_successful_execution(inv_info):
        return InvocationStatus.SUCCEEDED

    if _has_timed_out_execution(inv_info):
        return InvocationStatus.TIMED_OUT

    if _has_cancelled_execution(inv_info):
        return InvocationStatus.CANCELLED

    if _has_failed_attempts_equal_to_max_retries_limit(inv_info, num_failed_attempts):
        return InvocationStatus.FAILED

    # Can time out while waiting for spare worker capacity
    if _is_terminated_post_timeout(inv_info):
        return InvocationStatus.TIMED_OUT

    # Can be cancelled while waiting for spare worker capacity
    if inv_info.invocation_status == ApiInvocationStatus.TERMINATED:
        return InvocationStatus.CANCELLED

    # All TERMINATED cases have now been handled.
    # The remaining cases can only arise if the invocation is RUNNING.

    if _has_cancellation_request(inv_info):
        return InvocationStatus.CANCELLING

    if _has_execution_with_running_code(inv_info):
        return InvocationStatus.RUNNING

    return InvocationStatus.PENDING


def _has_successful_execution(inv_info: InvocationInfo) -> bool:
    return any(_is_successful_execution(execution) for execution in inv_info.executions)


def _has_timed_out_execution(inv_info: InvocationInfo) -> bool:
    return any(
        _is_aborted_execution(execution)
        and _received_sigterm_post_timeout(execution, inv_info)
        for execution in inv_info.executions
    )


def _has_cancelled_execution(inv_info: InvocationInfo) -> bool:
    return any(
        _is_aborted_execution(execution)
        and not _received_sigterm_post_timeout(execution, inv_info)
        for execution in inv_info.executions
    )


def _has_failed_attempts_equal_to_max_retries_limit(
    inv_info: InvocationInfo, num_failed_attempts: int
) -> bool:
    max_attempts = inv_info.execution_spec.max_retries + 1
    return num_failed_attempts == max_attempts


def _is_terminated_post_timeout(inv_info: InvocationInfo) -> bool:
    timeout_seconds = inv_info.execution_spec.timeout_seconds
    return (
        inv_info.invocation_status == ApiInvocationStatus.TERMINATED
        and inv_info.last_update_time > inv_info.creation_time + timeout_seconds
    )


def _has_cancellation_request(inv_info: InvocationInfo) -> bool:
    return inv_info.cancellation_request_time is not None


def _has_execution_with_running_code(inv_info: InvocationInfo) -> bool:
    # For an execution to be considered to have "running code", it's not enough to have
    # worker_status = RUNNING. We also need to exclude the possibilities that
    #   (a) the worker is still being provisioned
    #   (b) the worker has finished running code and is now being deprovisioned.
    return any(
        execution
        for execution in inv_info.executions
        if execution.worker_status == WorkerStatus.RUNNING
        and execution.execution_start_time is not None
        and execution.outcome is None
    )


def _get_num_failed_executions(invocation: InvocationInfo) -> int:
    return len(
        [
            execution
            for execution in invocation.executions
            if _is_failed_execution(execution)
        ]
    )


def _extract_result(inv_info: InvocationInfo) -> Optional[Any]:
    if len(inv_info.executions) == 0:
        return None

    latest_execution = max(inv_info.executions, key=_last_update_time_key)
    if latest_execution.output is None:
        return None

    return jsonpickle.decode(latest_execution.output)


def _extract_error(inv_info: InvocationInfo) -> Optional[str]:
    if len(inv_info.executions) == 0:
        return None

    latest_execution = max(inv_info.executions, key=_last_update_time_key)
    return cast(Optional[str], latest_execution.error_message)


def _last_update_time_key(execution: ExecutionSummary) -> int:
    return execution.last_update_time


# The functions:
# - _is_successful_execution,
# - _is_aborted_execution
# - _is_failed_execution
#
# 1) are mutually exclusive,
# 2) cover all executions that have an outcome, or are terminated, or both.


def _is_successful_execution(exec_info: ExecutionSummary) -> bool:
    return exec_info.outcome == ExecutionOutcome.SUCCEEDED


def _is_aborted_execution(exec_info: ExecutionSummary) -> bool:
    # Cases:
    # - Worker records the outcome as ABORTED.
    # - Worker receives SIGTERM before it has started executing (i.e. while it is spinning up)
    return exec_info.outcome == ExecutionOutcome.ABORTED or (
        exec_info.worker_status == WorkerStatus.TERMINATED
        and exec_info.outcome is None
        and (
            exec_info.termination_signal_time is not None
            and exec_info.execution_start_time is None
        )
    )


def _is_failed_execution(exec_info: ExecutionSummary) -> bool:
    # Cases:
    # - Worker records the outcome as FAILED
    # - Worker receives SIGTERM after it has started executing, and fails to clean up
    #     within grace period
    # - Worker suffers unexpected hardware failure
    return exec_info.outcome == ExecutionOutcome.FAILED or (
        exec_info.worker_status == WorkerStatus.TERMINATED
        and exec_info.outcome is None
        and not (
            exec_info.termination_signal_time is not None
            and exec_info.execution_start_time is None
        )
    )


def _received_sigterm_post_timeout(
    exec_info: ExecutionSummary, inv_info: InvocationInfo
) -> bool:
    if exec_info.termination_signal_time is None:
        return False

    return (
        exec_info.termination_signal_time
        > inv_info.creation_time + inv_info.execution_spec.timeout_seconds
    )
