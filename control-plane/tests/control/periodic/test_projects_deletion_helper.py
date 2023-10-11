from control_plane.control.periodic.projects_deletion_helper import (
    classify_projects_for_possible_deletion,
)
from control_plane.types.datatypes import (
    ExecutionSpec,
    FunctionStatus,
    InvocationInfo,
    InvocationStatus,
    ProjectInfo,
    ResourceSpec,
)

# project-1: undergoing deletion, with no running invocations
# project-2: undergoing deletion, but still has a running invocation
# project-3: not undergoing deletion

PROJECT_NAME_1 = "project-1"
PROJECT_NAME_2 = "project-2"
PROJECT_NAME_3 = "project-3"

VERSION_ID = "version"
FUNCTION_NAME = "function"
INVOCATION_ID = "invocation"

RESOURCE_SPEC = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=2)
EXECUTION_SPEC = ExecutionSpec(max_retries=5, timeout_seconds=100)

TIME = 0

PROJECTS: list[ProjectInfo] = [
    ProjectInfo(
        project_name=PROJECT_NAME_1, deletion_request_time=TIME, creation_time=TIME
    ),
    ProjectInfo(
        project_name=PROJECT_NAME_2, deletion_request_time=TIME, creation_time=TIME
    ),
    ProjectInfo(
        project_name=PROJECT_NAME_3, deletion_request_time=None, creation_time=TIME
    ),
]

RUNNING_INVOCATIONS: list[InvocationInfo] = [
    InvocationInfo(
        project_name=PROJECT_NAME_2,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.READY,
        prepared_function_details=None,
        input="input",
        cancellation_requested=False,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
        executions=[],
    )
]


def test_classify_projects_for_possible_deletion() -> None:
    classification = classify_projects_for_possible_deletion(
        PROJECTS, RUNNING_INVOCATIONS
    )

    names_of_projects_to_delete = {
        project.project_name for project in classification.projects_to_delete
    }
    names_of_projects_to_leave_untouched = {
        project.project_name for project in classification.projects_to_leave_untouched
    }

    assert names_of_projects_to_delete == {PROJECT_NAME_1}
    assert names_of_projects_to_leave_untouched == {PROJECT_NAME_2, PROJECT_NAME_3}
