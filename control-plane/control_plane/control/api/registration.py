from control_plane.control.utils.random_ids import generate_random_id
from control_plane.control.utils.version_reference_utils import (
    resolve_version_reference,
)
from control_plane.data.data_store import DataStore
from control_plane.types.datatypes import (
    ProjectInfo,
    ProjectsList,
    VersionDefinition,
    VersionInfo,
    VersionsListForProject,
    VersionReference,
    FunctionInfo,
    FunctionsListForVersion,
    FunctionStatus,
)


class RegistrationApiHandler:
    """
    API methods for registering new projects and new project versions.
    Called by the CLI tool.
    """

    def __init__(self, data_store: DataStore) -> None:
        self._data_store = data_store

    def create_project(self, project_name: str, time: int) -> ProjectInfo:
        """
        :raises ProjectAlreadyExists:
        """
        self._data_store.projects.create(project_name=project_name, creation_time=time)

        return self._data_store.projects.get(project_name=project_name)

    def get_project(self, project_name: str) -> ProjectInfo:
        """
        :raises ProjectDoesNotExist:
        """
        return self._data_store.projects.get(project_name=project_name)

    def list_projects(self) -> ProjectsList:
        return self._data_store.projects.list()

    def create_project_version(
        self, project_name: str, version_definition: VersionDefinition, time: int
    ) -> VersionInfo:
        """
        :raises ProjectDoesNotExist:
        """
        version_id = generate_random_id("ver")

        self._data_store.project_versions.create(
            project_name=project_name, version_id=version_id, creation_time=time
        )

        for function in version_definition.functions:
            self._data_store.functions.create(
                project_name=project_name,
                version_id=version_id,
                function_name=function.function_name,
                docker_image=(
                    function.docker_image_override
                    if function.docker_image_override is not None
                    else version_definition.default_docker_image
                ),
                resource_spec=function.resource_spec,
                execution_spec=function.execution_spec,
                function_status=FunctionStatus.PENDING,
                prepared_function_details=None,
            )

        return self._data_store.project_versions.get(
            project_name=project_name, version_id=version_id
        )

    def get_project_version(
        self, project_name: str, version_ref: VersionReference
    ) -> VersionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        return self._data_store.project_versions.get(
            project_name=project_name, version_id=version_id
        )

    def list_project_versions(self, project_name: str) -> VersionsListForProject:
        """
        :raises ProjectDoesNotExist:
        """
        return self._data_store.project_versions.list_for_project(
            project_name=project_name
        )

    def get_function(
        self, project_name: str, version_ref: VersionReference, function_name: str
    ) -> FunctionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        """
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        return self._data_store.functions.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
        )

    def list_functions(
        self, project_name: str, version_ref: VersionReference
    ) -> FunctionsListForVersion:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        return self._data_store.functions.list_for_project_version(
            project_name=project_name, version_id=version_id
        )
