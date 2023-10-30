import numpy as np
from fastapi import FastAPI
from multinode import get_deployed_function
from multinode.api_client.error_types import InvocationDoesNotExist

from multinode.core.invocation import InvocationStatus

app = FastAPI()
solve_tsp = get_deployed_function(
    project_name="tsp-solver",
    function_name="solve_tsp"
)


@app.post("/tsp")
def start_solving_tsp():
    invocation_id = solve_tsp.start(50, np.random.rand(50, 50))
    return f"Started TSP solved with invocation ID: {invocation_id}"


@app.get("/tsp/{invocation_id}")
def get_tsp_result(invocation_id: str):
    try:
        invocation = solve_tsp.get(invocation_id)
    except InvocationDoesNotExist:
        return f"Invocation with id {invocation_id} does not exist."

    if invocation.status == InvocationStatus.FAILED:
        msg = "Invocation failed. "
        if invocation.error is not None:
            msg += invocation.error
        return msg

    if invocation.status == InvocationStatus.PENDING:
        return "Provisioning resources for the invocation"

    if invocation.status == InvocationStatus.RUNNING:
        msg = "Invocation is running. "
        if invocation.result is not None:
            msg += "Best distance so far: " + str(invocation.result[1])
        return msg

    if invocation.status == InvocationStatus.CANCELLING:
        msg = "Invocation is being cancelled... "
        if invocation.result is not None:
            msg += "Best distance so far: " + str(invocation.result[1])
        return msg

    if invocation.status == InvocationStatus.CANCELLED:
        msg = "Invocation was cancelled. "
        if invocation.result is not None:
            msg += "Best distance found before cancellation: " + str(invocation.result[1])
        return msg

    if invocation.status == InvocationStatus.TIMED_OUT:
        msg = "Invocation timed out. "
        if invocation.result is not None:
            msg += "Best distance found before time out: " + str(invocation.result[1])
        return msg

    # Otherwise succeeded
    msg = "Invocation succeeded. "
    msg += "Best distance: " + str(invocation.result[1])
    return msg


@app.put("/tsp/{invocation_id}/cancel")
def cancel_tsp(invocation_id: str):
    solve_tsp.cancel(invocation_id)
    return "Cancellation request received"


@app.on_event("startup")
def startup_message():
    print(
        "Started travelling salesman problem (TSP) solver API.\n"
        "How to interact with the API from your terminal?\n"
        "\t- to start TSP task for 50 random cities run: `curl -X POST http://127.0.0.1:8000/tsp`\n"
        "\t- to get TSP task status run: `curl http://127.0.0.1:8000/tsp/INVOCATION_ID`\n"
        "\t- to cancel TSP task run: `curl -X PUT http://127.0.0.1:8000/tsp/INVOCATION_ID/cancel`\n"
    )
