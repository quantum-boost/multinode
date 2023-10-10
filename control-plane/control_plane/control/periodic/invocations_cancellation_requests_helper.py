from typing import NamedTuple

from pydantic import BaseModel

from control_plane.types.datatypes import InvocationInfo, ProjectInfo


class InvocationsClassificationForCancellationRequests(BaseModel):
    invocations_to_set_cancellation_requested: list[InvocationInfo]
    invocations_to_leave_untouched: list[InvocationInfo]


def classify_invocations_for_cancellation_requests(
    invocations: list[InvocationInfo], projects: list[ProjectInfo]
) -> InvocationsClassificationForCancellationRequests:
    invocations_to_set_cancellation_requested: list[InvocationInfo] = []
    invocations_to_leave_untouched: list[InvocationInfo] = []

    # {project_name: project_info}
    projects_by_name: dict[str, ProjectInfo] = _construct_dict_of_projects_by_name(
        projects
    )

    # Optimisation: iterate over invocations in the order in which they were created.
    # This usually means that we can propagate cancellation requests from grandparents
    # to grandchildren in a single pass, giving rise to better user experience.
    sorted_invocations = sorted(invocations, key=(lambda inv: inv.creation_time))

    invocations_cancelled_in_this_pass: set[InvocationIdentifier] = set()

    for invocation in sorted_invocations:
        if invocation.cancellation_requested:
            # Already has a cancellation request => No need to set it again
            invocations_to_leave_untouched.append(invocation)
        elif (
            invocation.project_name in projects_by_name
            and projects_by_name[invocation.project_name].deletion_requested
        ):
            # Project is being deleted, so cancel the invocation
            # NB the "invocation.project_name in projects_by_name" check is technically redundant -
            # we are coding defensively in case of future edits.
            invocations_to_set_cancellation_requested.append(invocation)
            invocations_cancelled_in_this_pass.add(_construct_identifier(invocation))
        elif _has_cancelled_parent(invocation, invocations_cancelled_in_this_pass):
            # Parent has a cancellation request, so cancel this invocation too
            invocations_to_set_cancellation_requested.append(invocation)
            invocations_cancelled_in_this_pass.add(_construct_identifier(invocation))
        else:
            # Default: do nothing
            invocations_to_leave_untouched.append(invocation)

    return InvocationsClassificationForCancellationRequests(
        invocations_to_set_cancellation_requested=invocations_to_set_cancellation_requested,
        invocations_to_leave_untouched=invocations_to_leave_untouched,
    )


class InvocationIdentifier(NamedTuple):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str


def _construct_identifier(invocation: InvocationInfo) -> InvocationIdentifier:
    return InvocationIdentifier(
        project_name=invocation.project_name,
        version_id=invocation.version_id,
        function_name=invocation.function_name,
        invocation_id=invocation.invocation_id,
    )


def _construct_identifier_of_parent(invocation: InvocationInfo) -> InvocationIdentifier:
    assert invocation.parent_invocation is not None
    return InvocationIdentifier(
        project_name=invocation.project_name,
        version_id=invocation.version_id,
        function_name=invocation.parent_invocation.function_name,
        invocation_id=invocation.parent_invocation.invocation_id,
    )


def _construct_dict_of_projects_by_name(
    projects: list[ProjectInfo],
) -> dict[str, ProjectInfo]:
    projects_by_name: dict[str, ProjectInfo] = dict()
    for project in projects:
        projects_by_name[project.project_name] = project
    return projects_by_name


def _has_cancelled_parent(
    invocation: InvocationInfo,
    invocations_cancelled_in_this_pass: set[InvocationIdentifier],
) -> bool:
    if invocation.parent_invocation is None:
        return False

    parent_cancelled_in_previous_pass = (
        invocation.parent_invocation.cancellation_requested
    )

    parent_cancelled_in_this_pass = (
        _construct_identifier_of_parent(invocation)
        in invocations_cancelled_in_this_pass
    )

    return parent_cancelled_in_previous_pass or parent_cancelled_in_this_pass
