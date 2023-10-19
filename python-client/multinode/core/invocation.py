from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, TypeVar, cast

import jsonpickle

from multinode.api_client import ExecutionOutcome, ExecutionSummary, InvocationInfo
from multinode.api_client import InvocationStatus as ApiInvocationStatus
from multinode.api_client import WorkerStatus

OutputT = TypeVar("OutputT")


class InvocationStatus(Enum):
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
    num_retries: int

    @staticmethod
    def from_invocation_info(inv_info: InvocationInfo) -> Invocation:
        num_retries = _get_num_failed_executions(inv_info)
        status = _resolve_invocation_status(inv_info, num_retries)
        terminated = inv_info.invocation_status == ApiInvocationStatus.TERMINATED
        result = _extract_result(inv_info)
        error = _extract_error(inv_info)
        return Invocation(
            status=status,
            result=result,
            error=error,
            terminated=terminated,
            num_retries=num_retries,
        )


def _resolve_invocation_status(
    inv_info: InvocationInfo, num_retries: int
) -> InvocationStatus:
    if _has_successful_execution(inv_info):
        return InvocationStatus.SUCCEEDED

    if _has_reached_max_retries_limit(inv_info, num_retries):
        return InvocationStatus.FAILED

    if _has_timed_out(inv_info):
        return InvocationStatus.TIMED_OUT

    if (
        _has_aborted_execution(inv_info)
        # If invocation was cancelled (and terminated) before any execution started
        # then there wouldn't be aborted executions.
        or inv_info.invocation_status == ApiInvocationStatus.TERMINATED
    ):
        return InvocationStatus.CANCELLED

    if _has_cancellation_request(inv_info):
        return InvocationStatus.CANCELLING

    if _has_execution_with_running_code(inv_info):
        return InvocationStatus.RUNNING

    return InvocationStatus.PENDING


def _has_successful_execution(inv_info: InvocationInfo) -> bool:
    return any(
        execution
        for execution in inv_info.executions
        if execution.outcome == ExecutionOutcome.SUCCEEDED
    )


def _has_reached_max_retries_limit(inv_info: InvocationInfo, num_retries: int) -> bool:
    max_attempts = inv_info.execution_spec.max_retries + 1
    return num_retries == max_attempts


def _has_timed_out(inv_info: InvocationInfo) -> bool:
    timeout_seconds = inv_info.execution_spec.timeout_seconds

    if (
        inv_info.invocation_status == ApiInvocationStatus.TERMINATED
        and inv_info.last_update_time > inv_info.creation_time + timeout_seconds
    ):
        return True

    # It could be the case that invocation hasn't terminated yet but the executions
    # already aborted due to timeout.
    termination_signal_times_of_aborted_executions = [
        execution.termination_signal_time
        for execution in inv_info.executions
        if execution.outcome == ExecutionOutcome.ABORTED
        and execution.termination_signal_time is not None
    ]
    return any(
        time > inv_info.creation_time + timeout_seconds
        for time in termination_signal_times_of_aborted_executions
    )


def _has_aborted_execution(inv_info: InvocationInfo) -> bool:
    return any(
        execution
        for execution in inv_info.executions
        if execution.outcome == ExecutionOutcome.ABORTED
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
    num_failures_due_to_code_exceptions = len(
        [
            execution
            for execution in invocation.executions
            if execution.outcome == ExecutionOutcome.FAILED
        ]
    )

    # Other reasons for failures include:
    #   (i) unexpected hardware failures
    #   (ii) worker process receiving SIGTERM and failing to clean up within the grace period
    # (or both (i) and (ii) can happen in the same execution)
    # Together, these cases correspond to worker_status = TERMINATED and outcome = None
    num_failures_due_to_other_reasons = len(
        [
            execution
            for execution in invocation.executions
            if (
                execution.worker_status == WorkerStatus.TERMINATED
                and execution.outcome is None
            )
        ]
    )

    return num_failures_due_to_code_exceptions + num_failures_due_to_other_reasons


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


def _last_update_time_key(execution: ExecutionSummary) -> float:
    return execution.last_update_time
