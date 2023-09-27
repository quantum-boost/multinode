from control_plane.types.datatypes import (
    ProjectInfo,
    ProjectsList,
    VersionDefinition,
    VersionInfo,
    VersionsList,
    VersionId,
    FunctionInfo,
)


class RegistrationApiHandler:
    """
    API methods for registering new projects and new project versions.
    Called by the CLI tool.
    """

    def create_project(self, project_name: str, time: int) -> ProjectInfo:
        """
        :raises ProjectAlreadyExists:
        """
        raise NotImplementedError

    def delete_project(self, project_name: str, time: int) -> None:
        """
        :raises ProjectStillHasActiveVersions:
        """
        raise NotImplementedError

    def get_project(self, project_name: str) -> ProjectInfo:
        """
        :raises ProjectDoesNotExist:
        """
        raise NotImplementedError

    def list_projects(self) -> ProjectsList:
        raise NotImplementedError

    def create_project_version(
        self, project_name: str, version_definition: VersionDefinition, time: int
    ) -> VersionInfo:
        """
        :raises ProjectDoesNotExist:
        """
        raise NotImplementedError

    def delete_project_version(
        self, project_name: str, version_id: VersionId, time: int
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises VersionStillHasIncompleteInvocations:
        """
        raise NotImplementedError

    def get_project_version(
        self, project_name: str, version_id: VersionId
    ) -> VersionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        raise NotImplementedError

    def list_project_versions(self, project_name: str) -> VersionsList:
        """
        :raises ProjectDoesNotExist:
        """
        raise NotImplementedError

    def get_function(
        self, project_name: str, version_id: VersionId, function_name: str
    ) -> FunctionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        """
        raise NotImplementedError

    def list_functions(self, project_name: str, version_id: VersionId) -> FunctionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        raise NotImplementedError
