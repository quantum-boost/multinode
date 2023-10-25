import inspect
import os
import time
from typing import Any, Callable, Generator, Iterable, List, Optional, Tuple

import jsonpickle
from pydantic import BaseModel

from multinode.api_client import (
    DefaultApi,
    FunctionSpec,
    InvocationDefinition,
    ParentInvocationDefinition,
)
from multinode.config import load_config_with_api_key_from_env_or_file
from multinode.core.invocation import Invocation, InvocationStatus
from multinode.errors import (
    FunctionInputSizeLimitExceeded,
    InvalidUseError,
    InvocationCancelledError,
    InvocationFailedError,
    InvocationTimedOutError,
)
from multinode.shared.parameter_bounds import INPUT_LENGTH_LIMIT
from multinode.shared.worker_environment_variables import (
    FUNCTION_NAME_ENV,
    INVOCATION_ID_ENV,
    PROJECT_NAME_ENV,
    VERSION_ID_ENV,
)
from multinode.utils.api import get_authenticated_client


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
        poll_frequency: int = 1,
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
        self._poll_frequency = poll_frequency

    def map(self, iterable: Iterable[Any]) -> Generator[Any, None, None]:
        """Call the Multinode function for each item in `iterable`.

        Similar to `map` or `multiprocessing.Pool.map` but each invocation is run
        on a distinct remote worker.

        :param iterable: arguments to pass to the functions
        :return: generator of results, in the same order as the inputs in `iterable`
        """
        return self.starmap([(item,) for item in iterable])

    def starmap(
        self, iterable: Iterable[Tuple[Any, ...]]
    ) -> Generator[Any, None, None]:
        """Call the Multinode function for each args tuple in `iterable`.

        Like `map` but each invocation can take more than one argument.

        Similar to `itertools.starmap` or `multiprocessing.Pool.starmap` but each
        invocation is run on a distinct remote worker.

        :param iterable: tuples of arguments to pass to the functions
        :return: generator of results, in the same order as the inputs in `iterable`
        """
        invocation_ids = [self.start(*args) for args in iterable]
        for inv_id in invocation_ids:
            yield self.await_result(inv_id)

    def call_remote(self, *args: Any, **kwargs: Any) -> Any:
        """Call the function on a remote worker and wait for the result.

        :param args: positional arguments to pass to the function
        :param kwargs: keyword arguments to pass to the function
        :return: result of the function call
        """
        invocation_id = self.start(*args, **kwargs)
        return self.await_result(invocation_id)

    def await_result(self, invocation_id: str) -> Any:
        """Wait for the result of a remote function call.

        :param invocation_id: id of the invocation to wait for
        :return: result of the function call
        """
        invocation = self.get(invocation_id)
        while not invocation.status.finished:
            time.sleep(self._poll_frequency)
            invocation = self.get(invocation_id)

        if invocation.status == InvocationStatus.FAILED:
            raise InvocationFailedError(invocation.error)

        if invocation.status == InvocationStatus.CANCELLED:
            raise InvocationCancelledError("Invocation was cancelled.")

        if invocation.status == InvocationStatus.TIMED_OUT:
            raise InvocationTimedOutError("Invocation timed out.")

        return invocation.result

    def call_local(self, *args: Any, **kwargs: Any) -> Any:
        """Call a function locally.

        :param args: positional arguments to pass to the function
        :param kwargs: keyword arguments to pass to the function
        :return: result of the function call
        """
        if self.fn is None:
            # It means function got obtained via `get_deployed_function(...)`
            # and we don't have the actual function definition
            # TODO can we somehow make it work in the future?
            raise InvalidUseError(
                "Functions obtained via `get_deployed_function(...)` "
                "cannot be called locally."
            )

        # For consistency with `call_remote`, `call_local` needs to return only the
        # final result, even if the function is a generator.
        if inspect.isgeneratorfunction(self.fn):
            results = list(self.fn(*args, **kwargs))  # Exhaust the generator
            if len(results) != 0:
                return results[-1]  # And return final result

            # Unless generator is empty, then result None
            return None
        else:
            return self.fn(*args, **kwargs)

    def start(self, *args: Any, **kwargs: Any) -> str:
        """Start a function on a remote worker and return its invocation id.

        :param args: positional arguments to pass to the function
        :param kwargs: keyword arguments to pass to the function
        :return: invocation id
        """
        data_for_inv = self._get_data_required_for_invocation()

        parent_invocation = _get_parent_invocation_from_env()

        serialized_input = jsonpickle.encode([args, kwargs])
        if len(serialized_input) > INPUT_LENGTH_LIMIT:
            raise FunctionInputSizeLimitExceeded("Function input exceeds size limit")

        inv_definition = InvocationDefinition(
            parent_invocation=parent_invocation, input=serialized_input
        )

        inv_info = data_for_inv.api_client.create_invocation(
            data_for_inv.project_name,
            data_for_inv.version_id,
            data_for_inv.fn_spec.function_name,
            inv_definition,
        )
        return inv_info.invocation_id

    def get(self, invocation_id: str) -> Invocation:
        """Get the status of a remote function call.

        :param invocation_id: id of the invocation to get
        :return: `Invocation` object describing current status of the invocation
        """
        data_for_inv = self._get_data_required_for_invocation()

        inv_info = data_for_inv.api_client.get_invocation(
            data_for_inv.project_name,
            data_for_inv.version_id,
            data_for_inv.fn_spec.function_name,
            invocation_id,
        )
        return Invocation.from_invocation_info(inv_info)

    def cancel(self, invocation_id: str) -> None:
        """Cancel a remote function call.

        :param invocation_id: id of the invocation to cancel
        """
        data_for_inv = self._get_data_required_for_invocation()
        data_for_inv.api_client.cancel_invocation(
            data_for_inv.project_name,
            data_for_inv.version_id,
            data_for_inv.fn_spec.function_name,
            invocation_id,
        )

    def list(self, offset: Optional[str] = None) -> InvocationIdsList:
        """List all invocations of a function.

        :param offset: starting list offset
        :return: list of invocation ids and the offset where the list ends if the
            list was too large to obtain in one API call
        """
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
            self._api_client = get_authenticated_client(config)

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
