from enum import Enum
from typing import Optional, NamedTuple

from pydantic import BaseModel, model_validator


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


class ExecutionSpec(BaseModel):
    max_retries: int
    timeout_seconds: int


class ExecutionLogs(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    execution_id: str
    log_lines: list[str]
    next_cursor: Optional[str]


class WorkerStatus(StrEnum):
    PENDING = "PENDING"
    PROVISIONING = "PROVISIONING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"


class WorkerType(StrEnum):
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
    latest_output: Optional[str]


class ExecutionFinalResultPayload(BaseModel):
    outcome: ExecutionOutcome
    final_output: Optional[str]
    error_message: Optional[str]


class ExecutionInfo(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    execution_id: str
    docker_image: str
    resource_spec: ResourceSpec
    execution_spec: ExecutionSpec
    function_status: FunctionStatus
    prepared_function_details: Optional[PreparedFunctionDetails]
    input: str
    cancellation_requested: bool
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


class InvocationIdentifier(BaseModel):
    function_name: str
    invocation_id: str


class InvocationDefinition(BaseModel):
    parent_invocation: Optional[InvocationIdentifier]
    input: str


class InvocationStatus(StrEnum):
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"


class InvocationInfo(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    parent_invocation: Optional[InvocationIdentifier]
    docker_image: str
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


class InvocationSummary(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    parent_invocation: Optional[InvocationIdentifier]
    cancellation_requested: bool
    invocation_status: InvocationStatus
    creation_time: int
    last_update_time: int


class InvocationsListForFunction(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocations: list[InvocationSummary]
    next_cursor: Optional[str]


class FunctionSpec(BaseModel):
    function_name: str
    docker_image_override: Optional[str]
    resource_spec: ResourceSpec
    execution_spec: ExecutionSpec


class FunctionInfoForVersion(BaseModel):
    # omit project_name and version_id, since this will be nested inside a VersionInfo object
    function_name: str
    docker_image: Optional[str]
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
        distinct_function_names = set(
            function.function_name for function in self.functions
        )
        if len(distinct_function_names) < len(self.functions):
            raise ValueError("Function names must be distinct")

        return self


class VersionInfo(BaseModel):
    project_name: str
    version_id: str
    creation_time: int
    functions: list[FunctionInfoForVersion]


class VersionsListForProject(BaseModel):
    project_name: str
    versions: list[VersionInfo]


class VersionReferenceType(Enum):
    NAMED = "NAMED"
    LATEST = "LATEST"


class VersionReference(NamedTuple):
    type: VersionReferenceType
    named_version_id: Optional[str]


class ProjectInfo(BaseModel):
    project_name: str
    creation_time: int


class ProjectsList(BaseModel):
    projects: list[ProjectInfo]
