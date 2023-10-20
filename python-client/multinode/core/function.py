import os
import time
from typing import Any, Callable, List, Optional, TypeVar

import jsonpickle
from pydantic import BaseModel

from multinode.api_client import (
    DefaultApi,
    FunctionSpec,
    InvocationDefinition,
    ParentInvocationDefinition,
)
from multinode.config import (
    create_control_plane_client_from_config,
    load_config_with_api_key_from_env_or_file,
)
from multinode.constants import (
    FUNCTION_NAME_ENV,
    INVOCATION_ID_ENV,
    PROJECT_NAME_ENV,
    VERSION_ID_ENV,
)
from multinode.core.errors import (
    InvalidUseError,
    InvocationCancelledError,
    InvocationFailedError,
    InvocationTimedOutError,
)
from multinode.core.invocation import Invocation, InvocationStatus

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class DataRequiredForInvocation(BaseModel):
    project_name: str
    version_id: str
    fn_spec: FunctionSpec
    api_client: DefaultApi

    class Config:
        # Otherwise pydantic complains about the `DefaultApi` type
        arbitrary_types_allowed = True


class InvocationIdsList(BaseModel):
    invocation_ids: List[str]
    next_offset: Optional[str]


class Function:
    def __init__(
        self,
        fn_spec: FunctionSpec,
        fn: Optional[Callable[..., Any]] = None,
        project_name: Optional[str] = None,
        version_id: Optional[str] = None,
    ):
        """
        There are 3 situations where Multinode Function object can be created and its
        methods called:
        1. When defining the Multinode deployment (using `mn.function` decorator) -
           in this case `project_name` and `version_id` are not known yet, so they will
           not be provided. In this case calling any of its method should fail and user
           should be informed to use `get_deployed_function(...)` method instead.
        2. When using `get_deployed_function(...)` - in this case `project_name` and
           `version_id` are explicitly provided by the user, and passed to the constructor.
        3. When multinode functions invoke other multinode functions - in this case
           `project_name` and `version_id` are loaded from environment variables.
        """
        self.fn_spec = fn_spec
        self.fn = fn
        self.project_name = project_name
        self.version_id = version_id
        self._api_client: Optional[DefaultApi] = None

    def call_remote(self, *input_data: Any, poll_frequency: float = 1) -> Any:
        invocation_id = self.start(*input_data)
        invocation = self.get(invocation_id)
        while not invocation.status.finished:
            time.sleep(poll_frequency)
            invocation = self.get(invocation_id)

        if invocation.status == InvocationStatus.FAILED:
            raise InvocationFailedError(invocation.error)

        if invocation.status == InvocationStatus.CANCELLED:
            raise InvocationCancelledError("Invocation was cancelled by a user.")

        if invocation.status == InvocationStatus.TIMED_OUT:
            raise InvocationTimedOutError(
                "Invocation did not finish before the timeout."
            )

        return invocation.result

    def call_local(self, *input_data: Any) -> Any:
        if self.fn is None:
            # It means function got obtained via `get_deployed_function(...)`
            # and we don't have the actual function definition
            # TODO can we somehow make it work in the future?
            raise InvalidUseError(
                "Functions obtained via `get_deployed_function(...)` "
                "cannot be called locally."
            )

        return self.fn(*input_data)

    def start(self, *input_data: Any) -> str:
        data_for_inv = self._get_data_required_for_invocation()

        parent_invocation = _get_parent_invocation_from_env()
        serialized_data = jsonpickle.encode(input_data)
        inv_definition = InvocationDefinition(
            parent_invocation=parent_invocation, input=serialized_data
        )

        inv_info = data_for_inv.api_client.create_invocation(
            data_for_inv.project_name,
            data_for_inv.version_id,
            data_for_inv.fn_spec.function_name,
            inv_definition,
        )
        return inv_info.invocation_id

    def get(self, invocation_id: str) -> Invocation:
        data_for_inv = self._get_data_required_for_invocation()

        inv_info = data_for_inv.api_client.get_invocation(
            data_for_inv.project_name,
            data_for_inv.version_id,
            data_for_inv.fn_spec.function_name,
            invocation_id,
        )
        return Invocation.from_invocation_info(inv_info)

    def cancel(self, invocation_id: str) -> None:
        data_for_inv = self._get_data_required_for_invocation()
        data_for_inv.api_client.cancel_invocation(
            data_for_inv.project_name,
            data_for_inv.version_id,
            data_for_inv.fn_spec.function_name,
            invocation_id,
        )

    def list(self, offset: Optional[str] = None) -> InvocationIdsList:
        data_for_inv = self._get_data_required_for_invocation()

        invocations_list = data_for_inv.api_client.list_invocations(
            data_for_inv.project_name,
            data_for_inv.version_id,
            data_for_inv.fn_spec.function_name,
            initial_offset=offset,
        )
        return InvocationIdsList(
            invocation_ids=[inv.invocation_id for inv in invocations_list.invocations],
            next_offset=invocations_list.next_offset,
        )

    def _get_data_required_for_invocation(self) -> DataRequiredForInvocation:
        if self.project_name is None:
            self.project_name = os.getenv(PROJECT_NAME_ENV)

        if self.version_id is None:
            self.version_id = os.getenv(VERSION_ID_ENV)

        # `project_name` and `version_id` should either be passed in the constructor
        # by the `get_deployed_function(...)` or they should be available as
        # env variables on the worker if the function is invoked by another function
        if self.project_name is None or self.version_id is None:
            raise InvalidUseError(
                "You tried to call a Multinode function returned by the "
                "@mn.function decorator directly from non-Multinode code. "
                "You should use `get_deployed_function(...)` function instead."
            )

        if self._api_client is None:
            config = load_config_with_api_key_from_env_or_file()
            self._api_client = create_control_plane_client_from_config(config)

        return DataRequiredForInvocation(
            project_name=self.project_name,
            version_id=self.version_id,
            fn_spec=self.fn_spec,
            api_client=self._api_client,
        )


def _get_parent_invocation_from_env() -> Optional[ParentInvocationDefinition]:
    parent_invocation_id = os.getenv(INVOCATION_ID_ENV)
    parent_function_name = os.getenv(FUNCTION_NAME_ENV)
    if parent_invocation_id is not None and parent_function_name is not None:
        return ParentInvocationDefinition(
            function_name=parent_function_name, invocation_id=parent_invocation_id
        )

    # If the env variables are not specified it means we're not running on a
    # multinode worker and the function is not invoked by other running invocation
    return None
