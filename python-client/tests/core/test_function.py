from unittest.mock import MagicMock

import pytest

from multinode.core.function import Function
from multinode.core.invocation import Invocation, InvocationStatus
from multinode.errors import (
    InvocationCancelledError,
    InvocationFailedError,
    InvocationTimedOutError,
)

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
    fn = Function(
        fn=fn_definition, fn_spec=MagicMock(), poll_frequency=TEST_POLL_FREQUENCY
    )
    fn.start = MagicMock(return_value="invocation_id")

    mocked_responses = [PENDING_INVOCATION, RUNNING_INVOCATION, SUCCEEDED_INVOCATION]
    fn.get = MagicMock(side_effect=mocked_responses)

    result = fn.call_remote("input-data")
    assert result == "final-result"


def test_call_remote_invocation_cancelled() -> None:
    fn = Function(
        fn=fn_definition, fn_spec=MagicMock(), poll_frequency=TEST_POLL_FREQUENCY
    )
    fn.start = MagicMock(return_value="invocation_id")

    mocked_responses = [CANCELLED_INVOCATIONS]
    fn.get = MagicMock(side_effect=mocked_responses)

    with pytest.raises(InvocationCancelledError):
        fn.call_remote("input-data")


def test_call_invocation_timed_out() -> None:
    fn = Function(
        fn=fn_definition, fn_spec=MagicMock(), poll_frequency=TEST_POLL_FREQUENCY
    )
    fn.start = MagicMock(return_value="invocation_id")

    mocked_responses = [
        RUNNING_INVOCATION,
        RUNNING_INVOCATION,
        RUNNING_INVOCATION,
        TIMED_OUT_INVOCATION,
    ]
    fn.get = MagicMock(side_effect=mocked_responses)

    with pytest.raises(InvocationTimedOutError):
        fn.call_remote("input-data")


def test_call_invocation_failed() -> None:
    fn = Function(
        fn=fn_definition, fn_spec=MagicMock(), poll_frequency=TEST_POLL_FREQUENCY
    )
    fn.start = MagicMock(return_value="invocation_id")

    mocked_responses = [
        PENDING_INVOCATION,
        PENDING_INVOCATION,
        PENDING_INVOCATION,
        FAILED_INVOCATION,
    ]
    fn.get = MagicMock(side_effect=mocked_responses)

    with pytest.raises(InvocationFailedError, match="error-message"):
        fn.call_remote("input-data")


def test_map_all_invocations_succeeded() -> None:
    n_inputs = 5
    fn = Function(
        fn=fn_definition, fn_spec=MagicMock(), poll_frequency=TEST_POLL_FREQUENCY
    )
    fn.start = MagicMock(side_effect=[f"invocation{i}" for i in range(n_inputs)])

    mocked_responses = [SUCCEEDED_INVOCATION] * n_inputs
    fn.get = MagicMock(side_effect=mocked_responses)

    result = fn.map(["input-data"] * n_inputs)
    for r in result:
        assert r == "final-result"


def test_starmap_all_invocations_succeed() -> None:
    n_inputs = 5
    fn = Function(
        fn=fn_definition, fn_spec=MagicMock(), poll_frequency=TEST_POLL_FREQUENCY
    )
    fn.start = MagicMock(side_effect=[f"invocation{i}" for i in range(n_inputs)])

    mocked_responses = [SUCCEEDED_INVOCATION] * n_inputs
    fn.get = MagicMock(side_effect=mocked_responses)

    result = fn.starmap([(f"first-arg{i}", f"second-arg{i}") for i in range(n_inputs)])
    for r in result:
        assert r == "final-result"


# TODO more tests for map and starmap
