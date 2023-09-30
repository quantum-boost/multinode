from control_plane.control.api.execution import ExecutionApiHandler
from control_plane.control.api.invocation import InvocationApiHandler
from control_plane.control.api.registration import RegistrationApiHandler
from control_plane.data.data_store import DataStore


class ApiHandler:
    def __init__(self, data_store: DataStore) -> None:
        self._registration = RegistrationApiHandler(data_store)
        self._invocation = InvocationApiHandler(data_store)
        self._execution = ExecutionApiHandler(data_store)

    @property
    def registration(self) -> RegistrationApiHandler:
        return self._registration

    @property
    def invocation(self) -> InvocationApiHandler:
        return self._invocation

    @property
    def execution(self) -> ExecutionApiHandler:
        return self._execution
