# Function definitions

Multinode is a framework for building applications that run asynchronous tasks.

In a Multinode project, each type of asynchronous task is defined as a Python function.


## Defining asynchronous tasks as Python functions

In the following script, the Python function `slowly_calculate_sum_of_squares` is decorated with an
`@mn.function()` decorator. This means that the Multinode framework will treat
`slowly_calculate_sum_of_squares` as a definition of an asynchronous task.

```python
# File: tasks/main.py

from multinode import Multinode
import time

mn = Multinode()

@mn.function()
def slowly_calculate_sum_of_squares(bound):
    sum = 0
    for i in range(bound):
        sum += i * i
        time.sleep(1)
    return sum
```

Once this function has been
[registered](./registration.md#registering-project-function-definitions)
with the Multinode control plane,
the function can be [invoked](./invocations.md#start) from other parts of the application
by passing in a value for the argument `bound`.

When invoked, a container is provisioned on the cloud, and the function code runs in this container.

When the function code completes, the return value `sum` is saved in the Multinode control plane,
and is [visible](./invocations.md#get) to other parts of the application.
The cloud container is then deprovisioned.


## Arguments and return values

Function arguments and return values must be pickleable.
(Multinode uses the `jsonpickle` library under the hood.)

It is common for arguments and return values to be of primitive types (e.g. `int` or `str`)
or collections types (e.g. `list[float]` or `dict[str, list[bool]]`).

It is also possible for an argument or return to be an instance of a custom Python class.
However, this only works if that class is defined in the codebase that is _invoking_ the function,
as well as in the codebase where the function is originally defined.

A function argument or return value must not exceed 250,000 characters in its pickled form.
If you want to pass a large dataset that exceeds this size limit,
then consider saving the dataset to cloud storage (e.g. S3) and passing the _location_
of the stored dataset as the function argument or return value.


## Configuration options

A function can be configured with specific CPU and memory requirements,
concurrency quota, timeout limit or retry policy.

```python
from multinode import Multinode
from datetime import timedelta

mn = Multinode()

@mn.function(
    cpu=16,
    memory="64 GiB",
    max_concurrency=25,
    timeout=timedelta(minutes=30),
    max_retries=3
)
def run_task(x):
    out =  # ... perform task
    return out
```

- `cpu`: The number of virtual CPUs made available to each function execution.
    Default: `0.1`. Limit: `16`.
- `memory`: The RAM made available to each function execution.
    Default: `"256 MiB"`. Limit: `"64 GiB"`.
- `timeout`: The maximum amount of time a function execution is allowed to run for.
    Default: `timedelta(hours=1)`, Limit: `timeout=timedelta(hours=24)`
- `max_concurrency`: The maximum number of function executions that can run simultaneously.
    If this quota is reached, then further function invocations will wait 
    in `PENDING` status until capacity become available.
    Default: `10`. Limit: `100`.
- `max_retries`: The maximum number of times the function can be retried in case of failure.
    Failures may be due to exceptions thrown in the code, or, in rare circumstances,
    due to hardware faults.
    Default: `0` (i.e. the function runs at most once). Limit: `50`.

If `max_retries` is set greater than `0`, then the function must be implemented
in an idempotent manner - meaning that if the code is retried,
the second execution will run cleanly and
produce the same result as if the first execution was successful.


## Exposing intermediate progress updates

A function can `yield` rather than `return`.
If `yield` is used, then the [result](./invocations.md#get) of a function invocation
that is visible to other parts of the application will be updated
whenever newer values are yielded by the function code.

This is useful for exposing progress updates from a long-running asynchronous task.

```python
from multinode import Multinode
import time

mn = Multinode()

@mn.function()
def slowly_calculate_sum_of_squares(bound):
    progress = {"num_terms_incorporated": 0, "sum_so_far": 0}
    
    for i in range(bound):
        progress["num_terms_incorporated"] += 1
        progress["sum_so_far"] += i * i
        
        yield progress
        
        time.sleep(1)
```


## Graceful handling of interruptions

If a function execution is interrupted due to a timeout or
[cancellation](./invocations.md#cancel), then a `InvocationCancelledError` or
`InvocationTimedOutError` is raised inside the function code.

The code is then given up to two minutes to terminate gracefully, after which
it will be forcibly killed.

```python
from multinode import Multinode
from multinode.errors import InvocationCancelledError, InvocationTimedOutError
from datetime import timedelta

mn = Multinode()

@mn.function(timeout=timedelta(minutes=30))
def run_function_that_terminates_gracefully(x):
    try:
        # ... perform calculation
    except InvocationCancelledError:
        print("Invocation is cancelled - gracefully terminating")
    except InvocationTimedOutError:
        print("Invocation timed out - gracefully terminating")
    finally:
        # ... perform cleanup
```


## Importing code from other modules

For large projects, you may wish to split your asynchronous task definition code across
multiple Python files.

Multinode requires your project to be structured in the following way.

```
[REPOSITORY ROOT]
└── tasks/
    ├── .env
    ├── requirements.txt
    ├── main.py
    └── submodule/
        ├── __init__.py
        ├── file_1.py
        └── file_2.py
```

The `main.py` file is mandatory. It contains all the functions that define asynchronous tasks
(i.e. all the functions with a `@mn.function()` decorator).

However, the `main.py` file may import code from other files in the codebase.


```python
# File: tasks/submodule/file_1.py

# No @mn.function() decorator - does not define an asynchronous task.
def square(i):
    return i * i
```

```python
# File: tasks/main.py

from multinode import Multinode
import time

# Pay attention to the import path!
from submodule.file_1 import square

mn = Multinode()

@mn.function()
def slowly_calculate_sum_of_squares(bound):
    sum = 0
    for i in range(bound):
        sum += square(i)
        time.sleep(1)
    return sum
    
@mn.function()
def slowly_calculate_product_of_squares(bound):
    product = 1
    for i in range(bound):
        product *= square(i)
        time.sleep(1)
    return product
```


## The Python environment

Multinode functions run inside containers built from the `python:3.8-bullseye` image.

If you need to include custom Python dependencies to this image, then add the dependencies to the
`requirements.txt` file in the [project folder](#project-folder-structure).

```
# File: tasks/requirements.txt

boto3==1.28.69
numpy==1.26.1
```


## Environment variables

Environment variables can be specified in the `.env` file in the [project folder](#project-folder-structure).

```
# File: tasks/requirements.txt

boto3==1.28.69
numpy==1.26.1
```


## Cloud permissions and networking

The IAM permissions and security group configurations for the function containers were set
when you installed Multinode into your AWS account using the [terraform](../../aws-infra/README.md).
They are not configured in your Python code.


---

**Next:** [Function registration](./registration.md)
