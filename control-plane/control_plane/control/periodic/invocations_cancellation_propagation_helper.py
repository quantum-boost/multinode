from typing import NamedTuple

from pydantic import BaseModel

from control_plane.types.datatypes import InvocationInfo


class InvocationsClassificationForCancellationPropagation(BaseModel):
    invocations_to_set_cancellation_requested: list[InvocationInfo]
    invocations_to_leave_untouched: list[InvocationInfo]


def classify_invocations_for_cancellation_propagation(
    invocations: list[InvocationInfo],
) -> InvocationsClassificationForCancellationPropagation:
    invocations_to_set_cancellation_requested: list[InvocationInfo] = []
    invocations_to_leave_untouched: list[InvocationInfo] = []

    # Optimisation: iterate over invocations in the order in which they were created.
    # This usually means that we can propagate cancellation requests from grandparents
    # to grandchildren in a single pass, giving rise to better user experience.
    sorted_invocations = sorted(invocations, key=(lambda inv: inv.creation_time))

    invocations_cancelled_in_this_pass: set[InvocationIdentifier] = set()

    for invocation in sorted_invocations:
        if not invocation.cancellation_requested:
            if invocation.parent_invocation is not None:
                parent_cancelled_in_previous_pass = invocation.parent_invocation.cancellation_requested
                parent_cancelled_in_this_pass = (
                    _construct_identifier_of_parent(invocation) in invocations_cancelled_in_this_pass
                )

                parent_cancelled = parent_cancelled_in_previous_pass or parent_cancelled_in_this_pass
                if parent_cancelled:
                    invocations_to_set_cancellation_requested.append(invocation)
                    invocations_cancelled_in_this_pass.add(_construct_identifier(invocation))
                    continue

        # Default:
        invocations_to_leave_untouched.append(invocation)

    return InvocationsClassificationForCancellationPropagation(
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
