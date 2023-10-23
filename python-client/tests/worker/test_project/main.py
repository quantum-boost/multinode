import os
import signal
from typing import Dict, Generator, Union

from test_project.errors.FailedFunctionError import FailedFunctionError
from test_project.length_calculation import calc_length
from yield_fn_final import YieldFnFinal

from multinode import Multinode
from multinode.errors import InvocationCancelledError

mn = Multinode()


@mn.function()
def return_function(x: str) -> int:
    return calc_length(x)


@mn.function()
def yield_function(
    x: str,
) -> Generator[Union[Dict[int, str], YieldFnFinal], None, None]:
    strings_so_far = []
    for i in range(len(x)):
        str_rn = {i: x[:i]}
        strings_so_far.append(str_rn)
        if i % 6 == 0:  # Report only every sixth string
            yield str_rn

    yield YieldFnFinal(strings_so_far)


@mn.function()
def yield_function_with_return(
    x: str,
) -> Generator[Union[Dict[int, str], YieldFnFinal], None, None]:
    for y in yield_function.call_local(x):
        yield y

    return {11: "return"}


@mn.function()
def failed_function(x: str) -> int:
    yield "intermediate-output"
    raise FailedFunctionError("I'm a failed function after all :(")


@mn.function()
def handled_aborted_function(x: str) -> Generator[str, None, None]:
    try:
        yield "pre-abort-output"
        os.kill(os.getpid(), signal.SIGTERM)
        yield "post-abort-output"
    except InvocationCancelledError:
        yield "cleanup-output"


@mn.function()
def unhandled_aborted_function(x: str) -> Generator[str, None, None]:
    yield "pre-abort-output"
    os.kill(os.getpid(), signal.SIGTERM)
    yield "post-abort-output"


@mn.function()
def failed_function_during_abort_handling(x: str) -> Generator[str, None, None]:
    try:
        yield "pre-abort-output"
        os.kill(os.getpid(), signal.SIGTERM)
        yield "post-abort-output"
    except InvocationCancelledError:
        yield "cleanup-pre-failure-output"
        raise FailedFunctionError("I'm a failed function after all :(")
        yield "cleanup-post-failure-output"
