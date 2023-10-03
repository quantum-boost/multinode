from typing import Optional

from pydantic import BaseModel

from control_plane.provisioning.http_helper import HttpRequestHandler
from control_plane.provisioning.provisioner import AbstractProvisioner, LogsResult
from control_plane.types.datatypes import ResourceSpec, PreparedFunctionDetails, WorkerDetails, WorkerStatus

# Current convention:
# Every request is POST /{verb}, with details in request body.

# Allowed verbs:
PREPARE_PATH = "/prepare"
PROVISION_PATH = "/provision"
TERMINATE_PATH = "/terminate"
CHECK_STATUS_PATH = "/check_status"
GET_LOGS_PATH = "/get_logs"


# Possible alternative convention:
# Requests can be
#  - POST /{resource_id}  (details in request body)
#  - POST /{resource_id}/{verb}   (details in request body)
#  - GET /{resource_id}?{query_param_1}={detail_1}&{query_param_2}={detail_2}
# with error status codes
#   - 404 when the resource does not exist
#   - 409 when the resource already exists
# This however makes the provisioner lambda fiddlier to implement, and I don't think that's worthwhile right now.


class PrepareFunctionRequest(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    docker_image: str
    resource_spec: ResourceSpec


class PrepareFunctionResponse(BaseModel):
    prepared_function_details: PreparedFunctionDetails

    class Config:
        extra = "ignore"


class ProvisionWorkerRequest(BaseModel):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    execution_id: str
    resource_spec: ResourceSpec
    prepared_function_details: PreparedFunctionDetails


class ProvisionWorkerResponse(BaseModel):
    worker_details: WorkerDetails

    class Config:
        extra = "ignore"


class TerminateWorkerRequest(BaseModel):
    worker_details: WorkerDetails


class TerminateWorkerResponse(BaseModel):
    class Config:
        extra = "ignore"


class CheckWorkerStatusRequest(BaseModel):
    worker_details: WorkerDetails


class CheckWorkerStatusResponse(BaseModel):
    worker_status: WorkerStatus

    class Config:
        extra = "ignore"


class GetLogsRequest(BaseModel):
    worker_details: WorkerDetails
    max_lines: int
    initial_offset: Optional[str] = None


class GetLogsResponse(BaseModel):
    log_lines: list[str]
    next_offset: Optional[str] = None

    class Config:
        extra = "ignore"


# Declaring request/response types in this code base is not ideal in the long term, seeing that
# our code base is acting as a client, not as a server. The mature long-term approach is to write
# an OpenAPI schema in a central location, and auto-generate *both* our client code and some
# skeleton code for each of the provisioner servers. I don't think this is a priority right now.


class ExternalProvisioner(AbstractProvisioner):
    """
    A provisioner that lives outside this codebase, which we communicate with via API calls.
    """

    def __init__(self, provisioner_api_url: str, provisioner_api_key: str):
        self._http_handler = HttpRequestHandler(base_url=provisioner_api_url, bearer_token=provisioner_api_key)

    def prepare_function(
        self, *, project_name: str, version_id: str, function_name: str, docker_image: str, resource_spec: ResourceSpec
    ) -> PreparedFunctionDetails:
        request_body = PrepareFunctionRequest(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            docker_image=docker_image,
            resource_spec=resource_spec,
        )

        response_body = self._http_handler.post(
            path=PREPARE_PATH, request_body=request_body, response_body_type=PrepareFunctionResponse
        )

        return response_body.prepared_function_details

    def provision_worker(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        resource_spec: ResourceSpec,
        prepared_function_details: PreparedFunctionDetails,
    ) -> WorkerDetails:
        request_body = ProvisionWorkerRequest(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
            resource_spec=resource_spec,
            prepared_function_details=prepared_function_details,
        )

        response_body = self._http_handler.post(
            path=PROVISION_PATH, request_body=request_body, response_body_type=ProvisionWorkerResponse
        )

        return response_body.worker_details

    def send_termination_signal_to_worker(self, *, worker_details: WorkerDetails) -> None:
        request_body = TerminateWorkerRequest(worker_details=worker_details)

        self._http_handler.post(
            path=TERMINATE_PATH, request_body=request_body, response_body_type=TerminateWorkerResponse
        )

    def check_worker_status(self, *, worker_details: WorkerDetails) -> WorkerStatus:
        request_body = CheckWorkerStatusRequest(worker_details=worker_details)

        response_body = self._http_handler.post(
            path=CHECK_STATUS_PATH, request_body=request_body, response_body_type=CheckWorkerStatusResponse
        )

        return response_body.worker_status

    def get_worker_logs(
        self, *, worker_details: WorkerDetails, max_lines: int, initial_offset: Optional[str]
    ) -> LogsResult:
        request_body = GetLogsRequest(worker_details=worker_details, max_lines=max_lines, initial_offset=initial_offset)

        response_body = self._http_handler.post(
            path=GET_LOGS_PATH, request_body=request_body, response_body_type=GetLogsResponse
        )

        return LogsResult(log_lines=response_body.log_lines, next_offset=response_body.next_offset)
