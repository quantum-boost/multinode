from control_plane.types.datatypes import ProjectInfo, ProjectsList


class ProjectsTable:
    def create(self, *, project_name: str, creation_time: int) -> None:
        """
        :raises ProjectAlreadyExists:
        """
        raise NotImplementedError

    def get(self, *, project_name: str) -> ProjectInfo:
        """
        :raises ProjectDoesNotExist:
        """
        raise NotImplementedError

    def list(self) -> ProjectsList:
        raise NotImplementedError
