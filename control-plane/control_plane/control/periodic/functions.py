from control_plane.data.data_store import DataStore
from control_plane.provisioning.provisioner import AbstractProvisioner
from control_plane.types.datatypes import FunctionStatus


class FunctionsLifecycleActions:
    """
    Control actions that progress Function objects through their lifecycle.
    These actions are executed periodically and in a single-threaded manner.
    """

    def __init__(self, data_store: DataStore, provisioner: AbstractProvisioner):
        self._data_store = data_store
        self._provisioner = provisioner

    def handle_pending_functions(self, time: int) -> None:
        """
        Prepare cloud resources so that it's possible to provision workers that execute these functions.
        """
        pending_functions = self._data_store.functions.list_all(
            statuses={FunctionStatus.PENDING}
        )

        for function in pending_functions:
            prepared_function_details = self._provisioner.prepare_function(
                project_name=function.project_name,
                version_id=function.version_id,
                function_name=function.function_name,
                docker_image=function.docker_image,
                resource_spec=function.resource_spec,
            )

            self._data_store.functions.update(
                project_name=function.project_name,
                version_id=function.version_id,
                function_name=function.function_name,
                new_status=FunctionStatus.READY,
                new_prepared_function_details=prepared_function_details,
            )
