from control_plane.data.data_store import DataStore
from control_plane.types.api_errors import ProjectIsBeingDeleted


def check_project_is_not_being_deleted(
    project_name: str, data_store: DataStore
) -> None:
    project = data_store.projects.get(project_name=project_name)
    if project.deletion_requested:
        raise ProjectIsBeingDeleted
