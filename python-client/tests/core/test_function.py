from unittest.mock import MagicMock

import pytest

from multinode.core.errors import (
    InvocationCancelledError,
    InvocationFailedError,
    InvocationTimedOutError,
)
from multinode.core.function import Function
from multinode.core.invocation import Invocation, InvocationStatus

TEST_POLL_FREQUENCY = 0.01

PENDING_INVOCATION = Invocation(
    status=InvocationStatus.PENDING,
    result=None,
    error=None,
    terminated=False,
    num_failed_attempts=0,
)

RUNNING_INVOCATION = Invocation(
    status=InvocationStatus.RUNNING,
    result="intermediate-result",
    error=None,
    terminated=False,
    num_failed_attempts=1,
)

SUCCEEDED_INVOCATION = Invocation(
    status=InvocationStatus.SUCCEEDED,
    result="final-result",
    error=None,
    # terminated=False on purpose, __call__ method should still return at this point
    terminated=False,
    num_failed_attempts=1,
)

CANCELLED_INVOCATIONS = Invocation(
    status=InvocationStatus.CANCELLED,
    result="cleanup-result",
    error=None,
    terminated=False,
    num_failed_attempts=0,
)

FAILED_INVOCATION = Invocation(
    status=InvocationStatus.FAILED,
    result="intermediate-result",
    error="error-message",
    terminated=False,
    num_failed_attempts=3,
)

TIMED_OUT_INVOCATION = Invocation(
    status=InvocationStatus.TIMED_OUT,
    result="intermediate-result",
    error=None,
    terminated=False,
    num_failed_attempts=3,
)


def fn_definition(x: str) -> int:
    return len(x)


def test_call_remote_invocation_succeeded() -> None:
    fn = Function(fn=fn_definition, fn_spec=MagicMock())
    fn.start = MagicMock(return_value="invocation_id")

    mocked_responses = [PENDING_INVOCATION, RUNNING_INVOCATION, SUCCEEDED_INVOCATION]
    fn.get = MagicMock(side_effect=mocked_responses)

    result = fn.call_remote("input-data", poll_frequency=TEST_POLL_FREQUENCY)
    assert result == "final-result"


def test_call_remote_invocation_cancelled() -> None:
    fn = Function(fn=fn_definition, fn_spec=MagicMock())
    fn.start = MagicMock(return_value="invocation_id")

    mocked_responses = [CANCELLED_INVOCATIONS]
    fn.get = MagicMock(side_effect=mocked_responses)

    with pytest.raises(InvocationCancelledError):
        fn.call_remote("input-data", poll_frequency=TEST_POLL_FREQUENCY)


def test_call_invocation_timed_out() -> None:
    fn = Function(fn=fn_definition, fn_spec=MagicMock())
    fn.start = MagicMock(return_value="invocation_id")

    mocked_responses = [
        RUNNING_INVOCATION,
        RUNNING_INVOCATION,
        RUNNING_INVOCATION,
        TIMED_OUT_INVOCATION,
    ]
    fn.get = MagicMock(side_effect=mocked_responses)

    with pytest.raises(InvocationTimedOutError):
        fn.call_remote("input-data", poll_frequency=TEST_POLL_FREQUENCY)


def test_call_invocation_failed() -> None:
    fn = Function(fn=fn_definition, fn_spec=MagicMock())
    fn.start = MagicMock(return_value="invocation_id")

    mocked_responses = [
        PENDING_INVOCATION,
        PENDING_INVOCATION,
        PENDING_INVOCATION,
        FAILED_INVOCATION,
    ]
    fn.get = MagicMock(side_effect=mocked_responses)

    with pytest.raises(InvocationFailedError, match="error-message"):
        fn.call_remote("input-data", poll_frequency=TEST_POLL_FREQUENCY)
