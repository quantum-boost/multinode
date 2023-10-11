from enum import Enum
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator


class StrEnum(str, Enum):
    def __repr__(self) -> str:
        return str.__repr__(self.value)

    def __str__(self) -> str:
        return str.__str__(self.value)


class ResourceSpec(BaseModel):
    virtual_cpus: float
    memory_gbs: float
    max_concurrency: int
    # can add GPUs in future

    @field_validator("virtual_cpus")
    @classmethod
    def check_valid_virtual_cpus(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("virtual_cpus must be positive")
        if value > 16:
            raise ValueError("virtual_cpus must not exceed 16")

        return value

    @field_validator("memory_gbs")
    @classmethod
    def check_valid_memory_gbs(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("memory_gbs must be positive")
        if value > 64:
            raise ValueError("memory_gbs must not exceed 64")

        return value

    @field_validator("max_concurrency")
    @classmethod
    def check_valid_max_concurrency(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("max_concurrency must be positive")
        if value > 100:
            raise ValueError("max_concurrency must not exceed 100")

        return value


class ExecutionSpec(BaseModel):
    max_retries: int
    timeout_seconds: int

    @field_validator("max_retries")
    @classmethod
    def check_valid_max_retries(cls, value: int) -> int:
        if value < 0:
            raise ValueError("max_retries must be non-negative")

        return value

    @field_validator("timeout_seconds")
    @classmethod
    def check_valid_timeout_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("timeout_seconds must be positive")
        if value > 86400:
            raise ValueError("timeout_seconds must not exceed 86400")

        return value


class ExecutionLogs(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    execution_id: str
    log_lines: list[str]
    next_offset: Optional[str]


class WorkerStatus(StrEnum):
    PENDING = "PENDING"
    PROVISIONING = "PROVISIONING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"


class WorkerType(StrEnum):
    TEST = "TEST"
    AWS_ECS = "AWS_ECS"
    # can add K8s in future


class WorkerDetails(BaseModel):
    type: WorkerType
    identifier: str  # e.g. ARN of ECS task
    logs_identifier: str  # e.g. ARN of Cloudwatch log stream for this ECS task


class PreparedFunctionDetails(BaseModel):
    type: WorkerType
    identifier: str  # e.g. the ARN of the ECS task definition


class ExecutionOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"  # i.e. SIGTERM signal received due to cancellation/timeout - aborted gracefully


class FunctionStatus(StrEnum):
    PENDING = "PENDING"
    READY = "READY"


class ExecutionTemporaryResultPayload(BaseModel):
    latest_output: str

    @field_validator("latest_output")
    @classmethod
    def check_latest_output_length(cls, value: str) -> str:
        if len(value) >= 16384:
            raise ValueError("latest_output cannot exceed 16384 characters")
        return value


class ExecutionFinalResultPayload(BaseModel):
    outcome: ExecutionOutcome
    final_output: Optional[str] = None
    error_message: Optional[str] = None

    @field_validator("final_output")
    @classmethod
    def check_final_output_length(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and len(value) >= 16384:
            raise ValueError("final_output cannot exceed 16384 characters")
        return value

    @field_validator("error_message")
    @classmethod
    def check_error_message_length(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and len(value) >= 16384:
            raise ValueError("error_message cannot exceed 16384 characters")
        return value


class ExecutionInfo(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    execution_id: str
    input: str
    cancellation_request_time: Optional[int]
    resource_spec: ResourceSpec
    execution_spec: ExecutionSpec
    function_status: FunctionStatus
    prepared_function_details: Optional[PreparedFunctionDetails]
    worker_status: WorkerStatus
    worker_details: Optional[WorkerDetails]
    termination_signal_time: Optional[int]
    outcome: Optional[ExecutionOutcome]
    output: Optional[str]
    error_message: Optional[str]
    creation_time: int
    last_update_time: int
    execution_start_time: Optional[int]
    execution_finish_time: Optional[int]
    invocation_creation_time: int

    @property
    def cancellation_requested(self) -> bool:
        return self.cancellation_request_time is not None

    @property
    def termination_signal_sent(self) -> bool:
        return self.termination_signal_time is not None

    @property
    def started(self) -> bool:
        return self.execution_start_time is not None

    @property
    def finished(self) -> bool:
        return self.execution_finish_time is not None


class ExecutionSummary(BaseModel):
    execution_id: str
    worker_status: WorkerStatus
    worker_details: Optional[WorkerDetails]
    termination_signal_time: Optional[int]
    outcome: Optional[ExecutionOutcome]
    output: Optional[str]
    error_message: Optional[str]
    creation_time: int
    last_update_time: int
    execution_start_time: Optional[int]
    execution_finish_time: Optional[int]

    @property
    def termination_signal_sent(self) -> bool:
        return self.termination_signal_time is not None

    @property
    def started(self) -> bool:
        return self.execution_start_time is not None

    @property
    def finished(self) -> bool:
        return self.execution_finish_time is not None


class ExecutionsListForInvocation(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    executions: list[ExecutionSummary]


class InvocationStatus(StrEnum):
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"


class ParentInvocationDefinition(BaseModel):
    function_name: str
    invocation_id: str


class InvocationDefinition(BaseModel):
    parent_invocation: Optional[ParentInvocationDefinition] = None
    input: str


class ParentInvocationInfo(BaseModel):
    function_name: str
    invocation_id: str
    cancellation_request_time: Optional[int]
    invocation_status: InvocationStatus
    creation_time: int
    last_update_time: int

    @property
    def cancellation_requested(self) -> bool:
        return self.cancellation_request_time is not None


class InvocationInfo(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    parent_invocation: Optional[ParentInvocationInfo]
    resource_spec: ResourceSpec
    execution_spec: ExecutionSpec
    function_status: FunctionStatus
    prepared_function_details: Optional[PreparedFunctionDetails]
    input: str
    cancellation_request_time: Optional[int]
    invocation_status: InvocationStatus
    creation_time: int
    last_update_time: int
    executions: list[ExecutionSummary]

    @property
    def cancellation_requested(self) -> bool:
        return self.cancellation_request_time is not None


class InvocationInfoForFunction(BaseModel):
    invocation_id: str
    parent_invocation: Optional[ParentInvocationDefinition]
    cancellation_request_time: Optional[int]
    invocation_status: InvocationStatus
    creation_time: int
    last_update_time: int

    @property
    def cancellation_requested(self) -> bool:
        return self.cancellation_request_time is not None


class InvocationsListForFunction(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocations: list[InvocationInfoForFunction]
    next_offset: Optional[str]


class FunctionSpec(BaseModel):
    function_name: str
    docker_image_override: Optional[str] = None
    resource_spec: ResourceSpec
    execution_spec: ExecutionSpec

    @field_validator("function_name")
    @classmethod
    def check_function_name_length(cls, value: str) -> str:
        if len(value) >= 256:
            raise ValueError("function_name cannot exceed 256 characters")
        return value

    @field_validator("docker_image_override")
    @classmethod
    def check_docker_image_override_length(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and len(value) >= 256:
            raise ValueError("docker_image_override cannot exceed 256 characters")
        return value


class FunctionInfoForVersion(BaseModel):
    # omit project_name and version_id, since this will be nested inside a VersionInfo object
    function_name: str
    docker_image: str
    resource_spec: ResourceSpec
    execution_spec: ExecutionSpec
    function_status: FunctionStatus
    prepared_function_details: Optional[PreparedFunctionDetails]


class FunctionInfo(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    docker_image: str
    resource_spec: ResourceSpec
    execution_spec: ExecutionSpec
    function_status: FunctionStatus
    prepared_function_details: Optional[PreparedFunctionDetails]
    project_deletion_request_time: Optional[int]

    @property
    def project_deletion_requested(self) -> bool:
        return self.project_deletion_request_time is not None


class FunctionsListForVersion(BaseModel):
    project_name: str
    version_id: str
    functions: list[FunctionInfoForVersion]


class VersionDefinition(BaseModel):
    default_docker_image: str
    functions: list[FunctionSpec]

    @field_validator("default_docker_image")
    @classmethod
    def check_default_docker_image_length(cls, value: str) -> str:
        if len(value) >= 256:
            raise ValueError("default_docker_image cannot exceed 256 characters")
        return value

    @model_validator(mode="after")
    def check_no_duplicate_function_names(self) -> "VersionDefinition":
        distinct_function_names = set(
            function.function_name for function in self.functions
        )
        if len(distinct_function_names) < len(self.functions):
            raise ValueError("function_name values must be distinct")

        return self


class VersionInfo(BaseModel):
    project_name: str
    version_id: str
    creation_time: int
    functions: list[FunctionInfoForVersion]


class VersionInfoForProject(BaseModel):
    # omit project_name
    version_id: str
    creation_time: int
    # omit functions too


class VersionsListForProject(BaseModel):
    project_name: str
    versions: list[VersionInfoForProject]


class ProjectInfo(BaseModel):
    project_name: str
    deletion_request_time: Optional[int]
    creation_time: int

    @property
    def deletion_requested(self) -> bool:
        return self.deletion_request_time is not None


class ProjectsList(BaseModel):
    projects: list[ProjectInfo]


class HealthStatus(BaseModel):
    status: str
