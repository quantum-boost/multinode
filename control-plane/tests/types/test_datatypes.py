import pytest
from pydantic import ValidationError

from control_plane.types.datatypes import (
    ExecutionFinalResultPayload,
    ExecutionOutcome,
    ExecutionSpec,
    FunctionSpec,
    ResourceSpec,
    VersionDefinition,
)


def test_valid_resource_spec() -> None:
    resource_spec = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=5)
    assert resource_spec.virtual_cpus == pytest.approx(1.0)
    assert resource_spec.memory_gbs == pytest.approx(4.0)
    assert resource_spec.max_concurrency == 5


def test_resource_spec_with_negative_cpus() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ResourceSpec(virtual_cpus=-1.0, memory_gbs=4.0, max_concurrency=5)

    assert "virtual_cpus must be positive" in str(exc_info.value)


def test_resource_spec_with_cpus_too_high() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ResourceSpec(virtual_cpus=9999.9, memory_gbs=4.0, max_concurrency=5)

    assert "virtual_cpus must not exceed" in str(exc_info.value)


def test_resource_spec_with_negative_memory() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ResourceSpec(virtual_cpus=1.0, memory_gbs=-4.0, max_concurrency=5)

    assert "memory_gbs must be positive" in str(exc_info.value)


def test_resource_spec_with_memory_too_high() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ResourceSpec(virtual_cpus=1.0, memory_gbs=9999.9, max_concurrency=5)

    assert "memory_gbs must not exceed" in str(exc_info.value)


def test_resource_spec_with_negative_max_concurrency() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ResourceSpec(virtual_cpus=1.0, memory_gbs=-4.0, max_concurrency=-5)

    assert "max_concurrency must be positive" in str(exc_info.value)


def test_resource_spec_with_max_concurrency_too_high() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ResourceSpec(virtual_cpus=1.0, memory_gbs=9999.9, max_concurrency=1000)

    assert "max_concurrency must not exceed" in str(exc_info.value)


def test_valid_execution_spec() -> None:
    execution_spec = ExecutionSpec(timeout_seconds=600, max_retries=0)
    assert execution_spec.timeout_seconds == 600
    assert execution_spec.max_retries == 0


def test_execution_spec_with_negative_timeout() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ExecutionSpec(timeout_seconds=-600, max_retries=0)

    assert "timeout_seconds must be positive" in str(exc_info.value)


def test_execution_spec_with_timeout_too_high() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ExecutionSpec(timeout_seconds=99999999, max_retries=0)

    assert "timeout_seconds must not exceed" in str(exc_info.value)


def test_execution_spec_with_negative_max_retries() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ExecutionSpec(timeout_seconds=600, max_retries=-1)

    assert "max_retries must be non-negative" in str(exc_info.value)


def test_valid_version_definition() -> None:
    version_def = VersionDefinition(
        default_docker_image="image:latest",
        functions=[
            FunctionSpec(
                function_name="foo",
                resource_spec=ResourceSpec(
                    virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=5
                ),
                execution_spec=ExecutionSpec(timeout_seconds=300, max_retries=1),
            ),
            FunctionSpec(
                function_name="bar",
                docker_image_override="other:2.0",
                resource_spec=ResourceSpec(
                    virtual_cpus=2.0, memory_gbs=8.0, max_concurrency=1
                ),
                execution_spec=ExecutionSpec(timeout_seconds=120, max_retries=0),
            ),
        ],
    )
    assert {function.function_name for function in version_def.functions} == {
        "foo",
        "bar",
    }


def test_version_definition_with_duplicate_function_names() -> None:
    with pytest.raises(ValidationError) as exc_info:
        VersionDefinition(
            default_docker_image="image:latest",
            functions=[
                FunctionSpec(
                    function_name="foo",
                    resource_spec=ResourceSpec(
                        virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=5
                    ),
                    execution_spec=ExecutionSpec(timeout_seconds=300, max_retries=1),
                ),
                FunctionSpec(
                    function_name="foo",
                    docker_image_override="other:2.0",
                    resource_spec=ResourceSpec(
                        virtual_cpus=2.0, memory_gbs=8.0, max_concurrency=1
                    ),
                    execution_spec=ExecutionSpec(timeout_seconds=120, max_retries=0),
                ),
            ],
        )

    assert "function_name values must be distinct" in str(exc_info.value)


def test_valid_final_result_payload_with_successful_outcome() -> None:
    ExecutionFinalResultPayload(outcome=ExecutionOutcome.SUCCEEDED, final_output="123")


def test_invalid_final_result_payload_with_successful_outcome() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.SUCCEEDED,
            final_output="123",
            error_message="unexpected",  # should not have an error message
        )

    assert "error_message must be left empty" in str(exc_info)


def test_valid_final_result_payload_with_failed_outcome() -> None:
    ExecutionFinalResultPayload(
        outcome=ExecutionOutcome.FAILED, final_output="123", error_message="reason"
    )


def test_invalid_final_result_payload_with_failed_outcome() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.FAILED,
            final_output="123",
            # missing an error message
        )

    assert "error_message must be populated" in str(exc_info)
