from abc import ABC, abstractmethod
from typing import Optional, NamedTuple

from control_plane.types.datatypes import (
    ResourceSpec,
    PreparedFunctionDetails,
    WorkerDetails,
    WorkerStatus,
)


class LogsResult(NamedTuple):
    log_lines: list[str]
    next_offset: Optional[str]


class AbstractProvisioner(ABC):
    @abstractmethod
    def prepare_function(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        docker_image: str,
        resource_spec: ResourceSpec
    ) -> PreparedFunctionDetails:
        """
        Create all necessary cloud resources so that it's possible to create workers that execute this function.
        """
        raise NotImplementedError

    @abstractmethod
    def provision_worker(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        resource_spec: ResourceSpec,
        prepared_function_details: PreparedFunctionDetails
    ) -> WorkerDetails:
        """
        Provision the worker on the cloud.
        """
        raise NotImplementedError

    @abstractmethod
    def send_termination_signal_to_worker(
        self, *, worker_details: WorkerDetails
    ) -> None:
        """
        Send a termination signal to the worker.
        """
        raise NotImplementedError

    @abstractmethod
    def check_worker_status(self, *, worker_details: WorkerDetails) -> WorkerStatus:
        """
        Check if the worker is still alive.
        (Should only ever return RUNNING or TERMINATED; should never return PENDING.)
        """
        raise NotImplementedError

    def notify_of_execution_completion(self, *, worker_details: WorkerDetails) -> None:
        """
        Only used in the mocked-up development provisioner.
        The purpose of this method is to simulate worker termination after executions complete.

        For real cloud provisioners, this method is a no-op.
        """
        # Override in development server
        pass

    @abstractmethod
    def get_worker_logs(
        self,
        *,
        worker_details: WorkerDetails,
        max_lines: int,
        initial_offset: Optional[str]
    ) -> LogsResult:
        """
        Get the logs outputted by the worker
        """
        raise NotImplementedError
