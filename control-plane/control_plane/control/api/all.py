from control_plane.control.api.execution import ExecutionApiHandler
from control_plane.control.api.invocation import InvocationApiHandler
from control_plane.control.api.logs import LogsApiHandler
from control_plane.control.api.registration import RegistrationApiHandler
from control_plane.data.data_store import DataStore
from control_plane.provisioning.provisioner import AbstractProvisioner


class ApiHandler:
    def __init__(self, data_store: DataStore, provisioner: AbstractProvisioner) -> None:
        self._registration = RegistrationApiHandler(data_store)
        self._invocation = InvocationApiHandler(data_store)
        self._execution = ExecutionApiHandler(data_store)
        self._logs = LogsApiHandler(data_store, provisioner)

    @property
    def registration(self) -> RegistrationApiHandler:
        return self._registration

    @property
    def invocation(self) -> InvocationApiHandler:
        return self._invocation

    @property
    def execution(self) -> ExecutionApiHandler:
        return self._execution

    @property
    def logs(self) -> LogsApiHandler:
        return self._logs
