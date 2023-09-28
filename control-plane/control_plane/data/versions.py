from control_plane.types.datatypes import VersionInfo, VersionsListForProject


class VersionsTable:
    def create(self, *, project_name: str, version_id: str, creation_time: int) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionAlreadyExists:
        """
        raise NotImplementedError

    def get_id_of_latest_version(self, *, project_name: str) -> str:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        raise NotImplementedError

    def get(self, *, project_name: str, version_id: str) -> VersionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        raise NotImplementedError

    def list_for_project(self, *, project_name: str) -> VersionsListForProject:
        """
        :raises ProjectDoesNotExist:
        """
        raise NotImplementedError
