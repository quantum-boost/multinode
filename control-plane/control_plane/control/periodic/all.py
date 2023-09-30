from control_plane.control.periodic.executions import ExecutionsLifecycleActions
from control_plane.control.periodic.functions import FunctionsLifecycleActions
from control_plane.control.periodic.invocations import InvocationsLifecycleActions
from control_plane.data.data_store import DataStore
from control_plane.provisioning.provisioner import AbstractProvisioner


class LifecycleActions:
    def __init__(self, data_store: DataStore, provisioner: AbstractProvisioner) -> None:
        self._functions = FunctionsLifecycleActions(data_store, provisioner)
        self._invocations = InvocationsLifecycleActions(data_store)
        self._executions = ExecutionsLifecycleActions(data_store, provisioner)

    def run_once(self, time: int) -> None:
        self._functions.run_all(time)
        self._invocations.run_all(time)
        self._executions.run_all(time)
