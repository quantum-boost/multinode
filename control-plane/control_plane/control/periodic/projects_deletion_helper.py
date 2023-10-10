from pydantic import BaseModel

from control_plane.types.datatypes import InvocationInfo, ProjectInfo


class ProjectsClassificationForDeletion(BaseModel):
    projects_to_delete: list[ProjectInfo]
    projects_to_leave_untouched: list[ProjectInfo]


def classify_projects_for_possible_deletion(
    projects: list[ProjectInfo], running_invocations: list[InvocationInfo]
) -> ProjectsClassificationForDeletion:
    names_of_projects_with_running_invocations = (
        _find_names_of_projects_with_running_invocations(running_invocations)
    )

    projects_to_delete: list[ProjectInfo] = []
    projects_to_leave_untouched: list[ProjectInfo] = []

    for project in projects:
        if project.project_name in names_of_projects_with_running_invocations:
            # If the project still has running invocations, then it is not safe to delete
            # (regardless of whether a deletion has been requested)
            projects_to_leave_untouched.append(project)
        elif project.deletion_requested:
            projects_to_delete.append(project)
        else:
            projects_to_leave_untouched.append(project)

    return ProjectsClassificationForDeletion(
        projects_to_delete=projects_to_delete,
        projects_to_leave_untouched=projects_to_leave_untouched,
    )


def _find_names_of_projects_with_running_invocations(
    running_invocations: list[InvocationInfo],
) -> set[str]:
    names_of_projects_with_running_invocations: set[str] = set()

    for invocation in running_invocations:
        names_of_projects_with_running_invocations.add(invocation.project_name)

    return names_of_projects_with_running_invocations
