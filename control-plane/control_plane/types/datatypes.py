from enum import Enum
from typing import Optional, Literal

from pydantic import BaseModel, model_validator, field_validator


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


class ExecutionFinalResultPayload(BaseModel):
    outcome: ExecutionOutcome
    final_output: Optional[str] = None
    error_message: Optional[str] = None


class ExecutionInfo(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    execution_id: str
    input: str
    cancellation_requested: bool
    resource_spec: ResourceSpec
    execution_spec: ExecutionSpec
    function_status: FunctionStatus
    prepared_function_details: Optional[PreparedFunctionDetails]
    worker_status: WorkerStatus
    worker_details: Optional[WorkerDetails]
    termination_signal_sent: bool
    outcome: Optional[ExecutionOutcome]
    output: Optional[str]
    error_message: Optional[str]
    creation_time: int
    last_update_time: int
    execution_start_time: Optional[int]
    execution_finish_time: Optional[int]
    invocation_creation_time: int


class ExecutionSummary(BaseModel):
    execution_id: str
    worker_status: WorkerStatus
    worker_details: Optional[WorkerDetails]
    termination_signal_sent: bool
    outcome: Optional[ExecutionOutcome]
    output: Optional[str]
    error_message: Optional[str]
    creation_time: int
    last_update_time: int
    execution_start_time: Optional[int]
    execution_finish_time: Optional[int]


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
    cancellation_requested: bool
    invocation_status: InvocationStatus
    creation_time: int
    last_update_time: int


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
    cancellation_requested: bool
    invocation_status: InvocationStatus
    creation_time: int
    last_update_time: int
    executions: list[ExecutionSummary]


class InvocationInfoForFunction(BaseModel):
    invocation_id: str
    parent_invocation: Optional[ParentInvocationDefinition]
    cancellation_requested: bool
    invocation_status: InvocationStatus
    creation_time: int
    last_update_time: int


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


class FunctionsListForVersion(BaseModel):
    project_name: str
    version_id: str
    functions: list[FunctionInfoForVersion]


class VersionDefinition(BaseModel):
    default_docker_image: str
    functions: list[FunctionSpec]

    @model_validator(mode="after")
    def check_no_duplicate_function_names(self) -> "VersionDefinition":
        distinct_function_names = set(function.function_name for function in self.functions)
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
    creation_time: int


class ProjectsList(BaseModel):
    projects: list[ProjectInfo]


class HealthStatus(BaseModel):
    status: str
