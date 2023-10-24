from __future__ import annotations

import inspect
import os
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any, Callable, Generator, NoReturn

import jsonpickle

from multinode.api_client import (
    DefaultApi,
    ExecutionFinalResultPayload,
    ExecutionOutcome,
    ExecutionTemporaryResultPayload,
)
from multinode.errors import (
    FunctionDoesNotExist,
    FunctionErrorMessageSizeLimitExceeded,
    FunctionOutputSizeLimitExceeded,
    InvocationCancelledError,
    InvocationTimedOutError,
)
from multinode.shared.parameter_bounds import (
    ERROR_MESSAGE_LENGTH_LIMIT,
    OUTPUT_LENGTH_LIMIT,
)
from multinode.shared.worker_environment_variables import (
    EXECUTION_ID_ENV,
    FUNCTION_NAME_ENV,
    INVOCATION_ID_ENV,
    PROJECT_NAME_ENV,
    VERSION_ID_ENV,
)
from multinode.utils.dynamic_imports import import_multinode_object_from_dir


@dataclass
class WorkerContext:
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str
    execution_id: str

    @staticmethod
    def from_env() -> WorkerContext:
        return WorkerContext(
            project_name=os.environ[PROJECT_NAME_ENV],
            version_id=os.environ[VERSION_ID_ENV],
            function_name=os.environ[FUNCTION_NAME_ENV],
            invocation_id=os.environ[INVOCATION_ID_ENV],
            execution_id=os.environ[EXECUTION_ID_ENV],
        )


class WorkerRunner:
    def __init__(self, api_client: DefaultApi, context: WorkerContext, root_dir: Path):
        self._api_client = api_client
        self._context = context
        self._root_dir = root_dir

        self._execution = self._api_client.get_execution(
            self._context.project_name,
            self._context.version_id,
            self._context.function_name,
            self._context.invocation_id,
            self._context.execution_id,
        )
        self._register_worker_signal_handler()
        self._aborted = False

    @property
    def outcome(self) -> ExecutionOutcome:
        if self._aborted:
            return ExecutionOutcome.ABORTED  # type: ignore

        return ExecutionOutcome.SUCCEEDED  # type: ignore

    def run_worker(self) -> None:
        self._start_execution()
        try:
            multinode_obj = import_multinode_object_from_dir(Path(self._root_dir))
            try:
                fn = multinode_obj._functions[self._context.function_name].fn
                assert fn is not None
            except KeyError:
                raise FunctionDoesNotExist(
                    f'Function "{self._context.function_name}" does not exist '
                    f'on version "{self._context.version_id}" of project '
                    f'"{self._context.project_name}".'
                )

            args, kwargs = jsonpickle.decode(self._execution.input)
            fn_out = fn(*args, **kwargs)
            if inspect.isgeneratorfunction(fn):
                self._process_generator_function_out(fn_out)
            else:
                self._process_classic_function_out(fn_out)

        except BaseException as e:
            if isinstance(e, InvocationCancelledError) or isinstance(
                e, InvocationTimedOutError
            ):
                final_result = ExecutionFinalResultPayload(
                    outcome=ExecutionOutcome.ABORTED  # type: ignore
                )
            else:
                final_result = ExecutionFinalResultPayload(
                    outcome=ExecutionOutcome.FAILED,  # type: ignore
                    error_message=_construct_error_message(e),
                )

            self._finish_execution(final_result)
            raise e  # Re-raise the error so the stack trace is visible in logs

    def _process_generator_function_out(
        self, fn_out: Generator[Any, None, None]
    ) -> None:
        for latest_out in fn_out:
            temp_result = ExecutionTemporaryResultPayload(
                latest_output=_serialize_output(latest_out)
            )
            self._update_execution(temp_result)

        final_result = ExecutionFinalResultPayload(outcome=self.outcome)
        self._finish_execution(final_result)

    def _process_classic_function_out(self, fn_out: Any) -> None:
        final_result = ExecutionFinalResultPayload(
            outcome=self.outcome, final_output=_serialize_output(fn_out)
        )
        self._finish_execution(final_result)

    def _start_execution(self) -> None:
        self._api_client.start_execution(
            project_name=self._context.project_name,
            version_ref_str=self._context.version_id,
            function_name=self._context.function_name,
            invocation_id=self._context.invocation_id,
            execution_id=self._context.execution_id,
        )

    def _update_execution(self, temp_result: ExecutionTemporaryResultPayload) -> None:
        self._api_client.update_execution(
            project_name=self._context.project_name,
            version_ref_str=self._context.version_id,
            function_name=self._context.function_name,
            invocation_id=self._context.invocation_id,
            execution_id=self._context.execution_id,
            execution_temporary_result_payload=temp_result,
        )

    def _finish_execution(self, final_result: ExecutionFinalResultPayload) -> None:
        self._api_client.finish_execution(
            project_name=self._context.project_name,
            version_ref_str=self._context.version_id,
            function_name=self._context.function_name,
            invocation_id=self._context.invocation_id,
            execution_id=self._context.execution_id,
            execution_final_result_payload=final_result,
        )

    def _register_worker_signal_handler(self) -> None:
        signal.signal(signal.SIGTERM, self._signal_handler())

    def _signal_handler(self) -> Callable[[Any, Any], Any]:
        def handle_signal(signum: int, frame: FrameType) -> NoReturn:
            self._aborted = True

            curr_time = int(time.time())
            timeout_time = (
                self._execution.invocation_creation_time
                + self._execution.execution_spec.timeout_seconds
            )
            if curr_time > timeout_time:
                raise InvocationTimedOutError()
            else:
                raise InvocationCancelledError()

        return handle_signal


def _serialize_output(output: Any) -> Any:
    serialized_output = jsonpickle.encode(output)
    if len(serialized_output) > OUTPUT_LENGTH_LIMIT:
        raise FunctionOutputSizeLimitExceeded("Function output exceeds size limit")
    return serialized_output


def _construct_error_message(e: BaseException) -> str:
    message = f"{e.__class__.__name__}: {str(e)}"
    if len(message) > ERROR_MESSAGE_LENGTH_LIMIT:
        raise FunctionErrorMessageSizeLimitExceeded(
            "Function error message exceeds size limit"
        )
    return message
