from control_plane.types.api_errors import ProjectNameIsTooLong


def check_project_name_length(project_name: str) -> None:
    if len(project_name) > 64:
        raise ProjectNameIsTooLong
