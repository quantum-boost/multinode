## Multinode

Multinode is a low-friction framework for running **asynchronous tasks** on the cloud. The framework:

- Provisions compute resources on demand, incurring zero costs when idle - all without you having to worry about
cloud API calls or cloud permissions.
- Handles retries, timeouts, concurrency quotas, cancellations and progress monitoring
- Supports distributed tasks of arbitrary complexity, i.e. tasks that spawn parallel subtasks
at runtime.
- Runs asynchronous tasks triggered by _users_ of an application - not just
offline/scheduled tasks.


### Quick start

Deploy the Multinode control plane into your AWS account - see [instructions here](aws-infra/README.md).
(Or contact us if you are interested in a hosted solution.)

Install the Multinode Python package and authenticate with the Multinode control plane.
```commandline
pip install multinode
multinode login
```

Define your asynchronous task as a Python function.
```python
# File: tasks/main.py

from multinode import Multinode
from datetime import timedelta

mn = Multinode()

@mn.function(
    cpu=4.0,
    memory="16 GiB",
    max_retries=1,
    max_concurrency=10,
    timeout=timedelta(hours=1)
)
def run_expensive_task(x):
    out =  # ... details of the task ...
    return out
```

Register the function with the Multinode control plane.
```commandline
multinode deploy tasks/ --project-name=my_project
```

Implement the rest of your application, which triggers the asynchronous task by invoking the Python function.
(In this particular example, the application is a FastAPI web server.)
```python
# File: application/main.py
# NB can be a different codebase from tasks/

from multinode import get_deployed_function
from fastapi import FastAPI

run_expensive_task = get_deployed_function(
    project_name="my_project",
    function_name="run_expensive_task"
)

app = FastAPI()

@app.post("/task_invocations")
def start_task():
    # The task will run on a *remote* cloud container (provisioned on demand)
    invocation_id = run_expensive_task.start(x=10000)
    return {"invocation_id": invocation_id}

@app.get("/task_invocations/{invocation_id}")
def get_task_status_and_result(invocation_id: str):
    invocation_data = run_expensive_task.get(invocation_id)
    return {
        "status": invocation_data.status,  # e.g. PENDING, RUNNING, SUCCEEDED, FAILED
        "result_if_complete": invocation_data.result
    }

@app.put("/task_invocations/{invocation_id}/cancel")
def cancel_task(invocation_id: str):
    run_expensive_task.cancel(invocation_id)
```

### Advanced usage

- [Client reference guide](python-client/README.md) - dependencies, project structure, deployment lifecycle and more.
- [An example with distributed compute](example-project/README.md) - subtasks spawned dynamically at runtime.


### Architecture

Currently, Multinode runs on **AWS**, using **ECS/Fargate** for the asynchronous tasks.

A (slightly simplified) architecture diagram is shown below

![architecture](images/architecture.png)

With minimal API changes, the framework can be extended to
other AWS compute engines (e.g. EC2 with GPUs), to other cloud providers, and to Kubernetes.

We may implement these extensions if there is demand. 
We also welcome contributions from the open source community in this regard.


### Resource provisioning - Multinode vs other solutions

**Multinode's approach: Direct resource provisioning.**
Multinode makes _direct API calls_ to the cloud provider, to provision a _new worker_ for _each new task_.

**Alternative approach: Autoscaling a warm worker pool.**
Popular alternative frameworks for asynchronous tasks include Celery and Kafka consumers.
Applications written in these frameworks usually run on a warm pool of workers.
Each worker stays alive between task executions.
The number of workers is _autoscaled_ according to some metric (e.g. the number of pending tasks).

**Advantages of Multinode's approach:**
- Scales up _immediately_ when new tasks are created; scales down _immediately_ when a task finishes.
- No risk of interrupting a task execution when scaling down.

**Advantages of the alternative warm-pool-based approach:**
- More suitable for processing a _higher volume_ of _shorter-lived_ tasks. 
- Can maintain spare capacity to mitigate against cold starts.


### Programming language support

Python is the only supported language at the moment.

If you need to _invoke_ a deployed Python function from an application written in
another language such as Javascript, then you will need to use the REST API.
(Or you can contribute a Javascript client!)

Let us know if you want to _define_ your functions in other languages.
