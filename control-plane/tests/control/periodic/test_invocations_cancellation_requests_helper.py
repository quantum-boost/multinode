from typing import Optional

from control_plane.control.periodic.invocations_cancellation_requests_helper import (
    InvocationsClassificationForCancellationRequests,
    classify_invocations_for_cancellation_requests,
)
from control_plane.types.datatypes import (
    ExecutionSpec,
    FunctionStatus,
    InvocationInfo,
    InvocationStatus,
    ParentInvocationInfo,
    ProjectInfo,
    ResourceSpec,
)

PROJECT_NAME = "project"
PROJECT_UNDERGOING_DELETION_NAME = "project-undergoing-deletion"
VERSION_ID = "version"
FUNCTION_NAME = "function"
INPUT = "input"
TIME = 0

RESOURCE_SPEC = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=2)
EXECUTION_SPEC = ExecutionSpec(max_retries=5, timeout_seconds=100)


PROJECTS = [
    ProjectInfo(
        project_name=PROJECT_NAME, deletion_request_time=None, creation_time=TIME
    ),
    ProjectInfo(
        project_name=PROJECT_UNDERGOING_DELETION_NAME,
        deletion_request_time=TIME,
        creation_time=TIME,
    ),
]


def create_invocation(
    invocation_id: str,
    parent: Optional[InvocationInfo],
    cancellation_requested: bool,
    creation_time: int = TIME,
    invocation_status: InvocationStatus = InvocationStatus.RUNNING,
    project_name: str = PROJECT_NAME,
) -> InvocationInfo:
    if parent is not None:
        parent_invocation_summary = ParentInvocationInfo(
            function_name=FUNCTION_NAME,
            invocation_id=parent.invocation_id,
            cancellation_requested=parent.cancellation_requested,
            invocation_status=parent.invocation_status,
            creation_time=TIME,
            last_update_time=TIME,
        )
    else:
        parent_invocation_summary = None

    return InvocationInfo(
        project_name=project_name,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=invocation_id,
        parent_invocation=parent_invocation_summary,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.READY,
        prepared_function_details=None,
        input=INPUT,
        cancellation_requested=cancellation_requested,
        invocation_status=invocation_status,
        creation_time=creation_time,
        last_update_time=TIME,
        executions=[],
    )


def assert_results(
    classification: InvocationsClassificationForCancellationRequests,
    expected_ids_to_set_cancellation_requested: Optional[set[str]] = None,
    expected_ids_to_leave_untouched: Optional[set[str]] = None,
) -> None:
    actual_ids_to_set_cancellation_requested = {
        invocation.invocation_id
        for invocation in classification.invocations_to_set_cancellation_requested
    }
    actual_ids_to_leave_untouched = {
        invocation.invocation_id
        for invocation in classification.invocations_to_leave_untouched
    }

    if expected_ids_to_set_cancellation_requested is None:
        assert len(actual_ids_to_set_cancellation_requested) == 0
    else:
        assert (
            actual_ids_to_set_cancellation_requested
            == expected_ids_to_set_cancellation_requested
        )

    if expected_ids_to_leave_untouched is None:
        assert len(actual_ids_to_leave_untouched) == 0
    else:
        assert actual_ids_to_leave_untouched == expected_ids_to_leave_untouched


def test_when_parent_is_cancelled_and_has_not_yet_been_cancelled_itself() -> None:
    parent_invocation_id = "parent"
    invocation_id = "inv"

    parent_invocation = create_invocation(
        parent_invocation_id, parent=None, cancellation_requested=True
    )
    invocation = create_invocation(
        invocation_id, parent=parent_invocation, cancellation_requested=False
    )

    running_invocations = [parent_invocation, invocation]

    classification = classify_invocations_for_cancellation_requests(
        running_invocations, PROJECTS
    )

    assert_results(
        classification,
        expected_ids_to_set_cancellation_requested={invocation_id},
        expected_ids_to_leave_untouched={parent_invocation_id},
    )


def test_with_two_invocation_with_one_cancelled_but_with_no_parent_child_relationship() -> (
    None
):
    invocation_id_1 = "inv-1"  # Cancelled, but not a parent of invocation 2
    invocation_id_2 = "inv-2"

    invocation_1 = create_invocation(
        invocation_id_1, parent=None, cancellation_requested=True
    )
    invocation_2 = create_invocation(
        invocation_id_2, parent=None, cancellation_requested=False
    )

    running_invocations = [invocation_1, invocation_2]

    classification = classify_invocations_for_cancellation_requests(
        running_invocations, PROJECTS
    )

    assert_results(
        classification,
        expected_ids_to_leave_untouched={invocation_id_1, invocation_id_2},
    )


def test_when_parent_is_cancelled_but_has_already_been_cancelled_itself() -> None:
    parent_invocation_id = "parent"
    invocation_id = "inv"

    parent_invocation = create_invocation(
        parent_invocation_id, parent=None, cancellation_requested=True
    )
    invocation = create_invocation(
        invocation_id, parent=parent_invocation, cancellation_requested=True
    )

    running_invocations = [parent_invocation, invocation]

    classification = classify_invocations_for_cancellation_requests(
        running_invocations, PROJECTS
    )

    assert_results(
        classification,
        expected_ids_to_leave_untouched={parent_invocation_id, invocation_id},
    )


def test_when_parent_is_not_cancelled() -> None:
    parent_invocation_id = "parent"
    invocation_id = "inv"

    parent_invocation = create_invocation(
        parent_invocation_id, parent=None, cancellation_requested=False
    )
    invocation = create_invocation(
        invocation_id, parent=parent_invocation, cancellation_requested=False
    )

    running_invocations = [parent_invocation, invocation]

    classification = classify_invocations_for_cancellation_requests(
        running_invocations, PROJECTS
    )

    assert_results(
        classification,
        expected_ids_to_leave_untouched={parent_invocation_id, invocation_id},
    )


def test_with_no_parent() -> None:
    invocation_id = "inv"

    invocation = create_invocation(
        invocation_id, parent=None, cancellation_requested=False
    )

    running_invocations = [invocation]

    classification = classify_invocations_for_cancellation_requests(
        running_invocations, PROJECTS
    )

    assert_results(classification, expected_ids_to_leave_untouched={invocation_id})


def test_with_parent_cancelled_and_already_terminated() -> None:
    parent_invocation_id = "parent"
    invocation_id = "inv"

    parent_invocation = create_invocation(
        parent_invocation_id,
        parent=None,
        cancellation_requested=True,
        invocation_status=InvocationStatus.TERMINATED,
    )
    invocation = create_invocation(
        invocation_id, parent=parent_invocation, cancellation_requested=False
    )

    # Parent is TERMINATED, so don't include in list of running invocations
    running_invocations = [invocation]

    classification = classify_invocations_for_cancellation_requests(
        running_invocations, PROJECTS
    )

    # Should still cancel the child
    assert_results(
        classification, expected_ids_to_set_cancellation_requested={invocation_id}
    )


def test_with_double_propagation_with_some_random_ordering() -> None:
    grandparent_invocation_id = "grandparent"  # cancelled
    parent_invocation_id = "parent"  # not yet cancelled
    invocation_id = "inv"  # not yet cancelled

    grandparent_invocation = create_invocation(
        grandparent_invocation_id,
        parent=None,
        cancellation_requested=True,
        creation_time=TIME,
    )
    parent_invocation = create_invocation(
        parent_invocation_id,
        parent=grandparent_invocation,
        cancellation_requested=False,
        creation_time=(TIME + 1),
    )
    invocation = create_invocation(
        invocation_id,
        parent=parent_invocation,
        cancellation_requested=False,
        creation_time=(TIME + 2),
    )

    # Put the invocations in the list in the wrong order.
    # The sorting by creation time in our implementation should ensure the double propagation
    # happens in a single loop iteration, leading to optimal user experience.
    invocations = [invocation, parent_invocation, grandparent_invocation]

    classification = classify_invocations_for_cancellation_requests(
        invocations, PROJECTS
    )

    assert_results(
        classification,
        expected_ids_to_set_cancellation_requested={
            invocation_id,
            parent_invocation_id,
        },
        expected_ids_to_leave_untouched={grandparent_invocation_id},
    )


def test_when_project_is_undergoing_deletion_and_invocation_has_not_yet_been_cancelled() -> (
    None
):
    invocation_id = "inv"

    invocation = create_invocation(
        invocation_id,
        parent=None,
        cancellation_requested=False,
        project_name=PROJECT_UNDERGOING_DELETION_NAME,
    )

    running_invocations = [invocation]

    classification = classify_invocations_for_cancellation_requests(
        running_invocations, PROJECTS
    )

    assert_results(
        classification, expected_ids_to_set_cancellation_requested={invocation_id}
    )


def test_when_project_is_undergoing_deletion_but_invocation_has_already_been_cancelled() -> (
    None
):
    invocation_id = "inv"

    invocation = create_invocation(
        invocation_id,
        parent=None,
        cancellation_requested=True,
        project_name=PROJECT_UNDERGOING_DELETION_NAME,
    )

    running_invocations = [invocation]

    classification = classify_invocations_for_cancellation_requests(
        running_invocations, PROJECTS
    )

    assert_results(classification, expected_ids_to_leave_untouched={invocation_id})
