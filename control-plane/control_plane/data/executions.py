from typing import Optional

from control_plane.types.datatypes import (
    WorkerStatus,
    WorkerDetails,
    ExecutionOutcome,
    ExecutionInfo,
    ExecutionsListForInvocation,
)


class ExecutionsTable:
    def create(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        worker_status: WorkerStatus,
        worker_details: Optional[WorkerDetails],
        termination_signal_sent: bool,
        outcome: Optional[ExecutionOutcome],
        output: Optional[str],
        error_message: Optional[str],
        creation_time: int,
        last_update_time: int,
        execution_start_time: Optional[int],
        execution_end_time: Optional[int]
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionAlreadyExists:
        """
        raise NotImplementedError

    def update(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        update_time: int,
        new_worker_status: Optional[WorkerStatus] = None,
        new_worker_details: Optional[WorkerDetails] = None,
        set_termination_signal_sent: bool = False,
        new_outcome: Optional[ExecutionOutcome] = None,
        new_output: Optional[str] = None,
        new_error_message: Optional[str] = None,
        execution_start_time: Optional[int] = None,
        execution_end_time: Optional[int] = None,
        should_already_have_started: Optional[bool] = None,
        should_already_have_finished: Optional[bool] = None
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        :raises ExecutionHasNotStarted:
        :raises ExecutionHasAlreadyStarted:
        :raises ExecutionHasAlreadyFinished:
        :raises ExecutionHasNotFinished:
        """
        raise NotImplementedError

    def get(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        """
        raise NotImplementedError

    def list_for_invocation(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str
    ) -> ExecutionsListForInvocation:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        """
        raise NotImplementedError

    def list_all(self, *, worker_statuses: set[WorkerStatus]) -> list[ExecutionInfo]:
        """
        The worker_statuses argument must be populated. It is unwise to call this method with the TERMINATED status,
        since that is likely to return a very large number of results.
        """
        raise NotImplementedError
