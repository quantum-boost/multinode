"""
A basic API, used to test our build process. Will eventually replace with real code.
"""
import logging
from typing import Optional

import uvicorn
from fastapi import FastAPI, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.requests import Request
from starlette.responses import JSONResponse

from control_plane.control.api.all import ApiHandler
from control_plane.control.utils.version_reference import parse_version_reference
from control_plane.data.data_store import DataStore
from control_plane.entrypoints.utils.authenticator_setup import authenticator_from_environment_variables
from control_plane.entrypoints.utils.cli_arguments import parse_cli_arguments
from control_plane.entrypoints.utils.current_time import current_time
from control_plane.entrypoints.utils.documentation import document_possible_errors
from control_plane.entrypoints.utils.provisioner_setup import provisioner_from_environment_variables
from control_plane.entrypoints.utils.sql_setup import datastore_from_environment_variables
from control_plane.provisioning.provisioner import AbstractProvisioner
from control_plane.types.datatypes import (
    ProjectInfo,
    ProjectsList,
    VersionInfo,
    VersionDefinition,
    VersionsListForProject,
    InvocationInfo,
    InvocationDefinition,
    InvocationsListForFunction,
    ExecutionInfo,
    ExecutionTemporaryResultPayload,
    ExecutionFinalResultPayload,
    InvocationStatus,
    HealthStatus,
    ExecutionLogs,
)
from control_plane.types.errortypes import (
    ApiError,
    ProjectAlreadyExists,
    ApiKeyIsInvalid,
    ProjectDoesNotExist,
    VersionDoesNotExist,
    FunctionDoesNotExist,
    ParentInvocationDoesNotExist,
    InvocationDoesNotExist,
    ExecutionDoesNotExist,
    ExecutionHasAlreadyStarted,
    ExecutionHasNotStarted,
    ExecutionHasAlreadyFinished,
    OffsetIsInvalid,
    ParentFunctionNameIsMissing,
    ParentInvocationIdIsMissing,
)
from control_plane.types.parent_invocation_helper import parse_parent_invocation_definition
from control_plane.user_management.authenticator import AbstractAuthenticator, AuthResult

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# fmt: off
def build_app(data_store: DataStore, provisioner: AbstractProvisioner, authenticator: AbstractAuthenticator) -> FastAPI:
    api_handler = ApiHandler(data_store, provisioner)

    app = FastAPI()
    app.openapi_version = "3.0.3"  # has better compatibility with client generators

    security = HTTPBearer()

    def authenticate(authorization: HTTPAuthorizationCredentials = Depends(security)) -> AuthResult:
        return authenticator.authenticate(authorization.credentials)

    @app.get(
        path="/",
        responses=document_possible_errors([])
    )
    def health_check() -> HealthStatus:
        return HealthStatus(status="healthy")

    # Registration endpoints - called by CLI tool

    @app.put(
        path="/projects/{project_name}",
        responses=document_possible_errors([ProjectAlreadyExists, ApiKeyIsInvalid]),
    )
    def create_project(project_name: str, auth_info: AuthResult = Depends(authenticate)) -> ProjectInfo:
        return api_handler.registration.create_project(project_name, current_time())

    @app.get(
        path="/projects/{project_name}",
        responses=document_possible_errors([ProjectDoesNotExist, ApiKeyIsInvalid])
    )
    def get_project(project_name: str, auth_info: AuthResult = Depends(authenticate)) -> ProjectInfo:
        return api_handler.registration.get_project(project_name)

    @app.get(
        path="/projects",
        responses=document_possible_errors([ApiKeyIsInvalid])
    )
    def list_projects(auth_info: AuthResult = Depends(authenticate)) -> ProjectsList:
        return api_handler.registration.list_projects()

    @app.post(
        path="/projects/{project_name}/versions",
        responses=document_possible_errors([ProjectDoesNotExist, ApiKeyIsInvalid]),
    )
    def create_project_version(
        project_name: str, version_definition: VersionDefinition, auth_info: AuthResult = Depends(authenticate)
    ) -> VersionInfo:
        return api_handler.registration.create_project_version(project_name, version_definition, current_time())

    @app.get(
        path="/projects/{project_name}/versions/{version_ref_str}",
        responses=document_possible_errors([ProjectDoesNotExist, VersionDoesNotExist, ApiKeyIsInvalid]),
    )
    def get_project_version(
        project_name: str, version_ref_str: str, auth_info: AuthResult = Depends(authenticate)
    ) -> VersionInfo:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.registration.get_project_version(project_name, version_ref)

    @app.get(
        path="/projects/{project_name}/versions",
        responses=document_possible_errors([ProjectDoesNotExist, ApiKeyIsInvalid]),
    )
    def list_project_versions(
            project_name: str,
            auth_info: AuthResult = Depends(authenticate)
    ) -> VersionsListForProject:
        return api_handler.registration.list_project_versions(project_name)

    # Invocation endpoints - called by internal/external code invoking the functions

    @app.post(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/invocations",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist, ParentInvocationDoesNotExist,
                ApiKeyIsInvalid,
            ]
        ),
    )
    def create_invocation(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        invocation_def: InvocationDefinition,
        auth_info: AuthResult = Depends(authenticate),
    ) -> InvocationInfo:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.invocation.create_invocation(
            project_name, version_ref, function_name, invocation_def, current_time()
        )

    @app.put(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/"
             "invocations/{invocation_id}/cancel",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist, InvocationDoesNotExist, ApiKeyIsInvalid,
            ]
        ),
    )
    def cancel_invocation(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        invocation_id: str,
        auth_info: AuthResult = Depends(authenticate),
    ) -> InvocationInfo:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.invocation.cancel_invocation(
            project_name, version_ref, function_name, invocation_id, current_time()
        )

    @app.get(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/"
             "invocations/{invocation_id}",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist, InvocationDoesNotExist, ApiKeyIsInvalid,
            ]
        ),
    )
    def get_invocation(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        invocation_id: str,
        auth_info: AuthResult = Depends(authenticate),
    ) -> InvocationInfo:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.invocation.get_invocation(project_name, version_ref, function_name, invocation_id)

    @app.get(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/invocations",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist, OffsetIsInvalid,
                ParentFunctionNameIsMissing, ParentInvocationIdIsMissing, ApiKeyIsInvalid,
            ]
        ),
    )
    def list_invocations(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        max_results: Optional[int] = None,
        initial_offset: Optional[str] = None,
        status: Optional[InvocationStatus] = None,
        parent_function_name: Optional[str] = None,
        parent_invocation_id: Optional[str] = None,
        auth_info: AuthResult = Depends(authenticate),
    ) -> InvocationsListForFunction:
        version_ref = parse_version_reference(version_ref_str)
        parent_invocation = parse_parent_invocation_definition(parent_function_name, parent_invocation_id)
        return api_handler.invocation.list_invocations(
            project_name, version_ref, function_name, max_results, initial_offset, status, parent_invocation
        )

    # Execution endpoints - called by workers running the functions

    @app.put(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/"
             "invocations/{invocation_id}/executions/{execution_id}/start",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist, InvocationDoesNotExist,
                ExecutionDoesNotExist, ExecutionHasAlreadyStarted, ApiKeyIsInvalid,
            ]
        ),
    )
    def start_execution(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        auth_info: AuthResult = Depends(authenticate),
    ) -> ExecutionInfo:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.execution.mark_execution_as_started(
            project_name, version_ref, function_name, invocation_id, execution_id, current_time()
        )

    @app.put(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/"
        "invocations/{invocation_id}/executions/{execution_id}/update",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist, InvocationDoesNotExist,
                ExecutionDoesNotExist, ExecutionHasNotStarted, ExecutionHasAlreadyFinished, ApiKeyIsInvalid,
            ]
        ),
    )
    def update_execution(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        temp_result: ExecutionTemporaryResultPayload,
        auth_info: AuthResult = Depends(authenticate),
    ) -> ExecutionInfo:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.execution.upload_temporary_execution_result(
            project_name, version_ref, function_name, invocation_id, execution_id, temp_result, current_time()
        )

    @app.put(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/"
             "invocations/{invocation_id}/executions/{execution_id}/finish",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist, InvocationDoesNotExist,
                ExecutionDoesNotExist, ExecutionHasNotStarted, ExecutionHasAlreadyFinished, ApiKeyIsInvalid,
            ]
        ),
    )
    def finish_execution(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        final_result: ExecutionFinalResultPayload,
        auth_info: AuthResult = Depends(authenticate),
    ) -> ExecutionInfo:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.execution.set_final_execution_result(
            project_name, version_ref, function_name, invocation_id, execution_id, final_result, current_time()
        )

    @app.get(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/"
             "invocations/{invocation_id}/executions/{execution_id}",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist,
                InvocationDoesNotExist, ExecutionDoesNotExist, ApiKeyIsInvalid,
            ]
        ),
    )
    def get_execution(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        auth_info: AuthResult = Depends(authenticate),
    ) -> ExecutionInfo:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.execution.get_execution(
            project_name, version_ref, function_name, invocation_id, execution_id
        )

    # Logs endpoint

    @app.get(
        path="/projects/{project_name}/versions/{version_ref_str}/functions/{function_name}/"
             "invocations/{invocation_id}/executions/{execution_id}/logs",
        responses=document_possible_errors(
            [
                ProjectDoesNotExist, VersionDoesNotExist, FunctionDoesNotExist,
                InvocationDoesNotExist, ExecutionDoesNotExist, ApiKeyIsInvalid,
            ]
        ),
    )
    def get_execution_logs(
        project_name: str,
        version_ref_str: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        max_lines: Optional[int] = None,
        initial_offset: Optional[str] = None,
        auth_info: AuthResult = Depends(authenticate),
    ) -> ExecutionLogs:
        version_ref = parse_version_reference(version_ref_str)
        return api_handler.logs.get_execution_logs(
            project_name, version_ref, function_name, invocation_id, execution_id, max_lines, initial_offset
        )

    # Error handling

    @app.exception_handler(ApiError)
    def handle_worker_does_not_exist(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.error_code(), content=exc.response().model_dump())

    return app
# fmt: on


def main() -> None:
    cli_args = parse_cli_arguments()
    provisioner = provisioner_from_environment_variables(cli_args)
    authenticator = authenticator_from_environment_variables()

    with datastore_from_environment_variables(cli_args) as data_store:
        app = build_app(data_store, provisioner, authenticator)
        uvicorn.run(app, host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()
