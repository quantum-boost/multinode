# Function invocations

In the previous sections, we explained how to [define](./definitions.md) asynchronous tasks as Python functions,
and how to [register](./registration.md) these Python functions with the Multinode control plane 
using the CLI tool.

This section explains how you can _invoke_ these functions from other parts of your application.


## Invoking functions from other parts of the application

A typical Multinode project is structured in the following manner.

```
[REPOSITORY ROOT]
├── tasks/
│   ├── .env
│   ├── requirements.txt
│   ├── main.py
│   └── submodule/
│       ├── __init__.py
│       ├── file_1.py
│       └── file_2.py
└── application/
    ├── main.py
    └── submodule/
        ├── __init__.py
        ├── file_1.py
        └── file_2.py
```

- `tasks/` contains the functions that _define_ the asynchronous tasks, i.e. the functions
that carry an `@mn.function()` decorator.
This folder is packaged and uploaded to the Multinode control plane when you run the `multinode deploy`
[CLI command](./registration.md#registering-project-function-definitions).
- `application/` contains the remainder of the application, including code that _invokes_
functions from `tasks/`.

Suppose that `tasks/main.py` contains a function called `slowly_calculate_sum_of_squares`.
This function carries an `@mn.function()` decorator, which means that it represents an asynchronous task.

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

Suppose that the code in `tasks/` has been registered with the control plane,
under the project name `my_project`.

```commandline
multinode deploy tasks/ project-name=my_project
```

Then the `slowly_calculate_sum_of_squares` function can be invoked from within `application/main.py`.

```python
# File: application/main.py

from multinode import get_deployed_function

slowly_calculate_sum_of_squares = get_deployed_function(
    project_name="my_project",
    function_name="slowly_calculate_sum_of_squares"
)

# ... other code ...

# Starts an invocation of slowly_calculate_sum_of_squares, which runs *remotely*.
invocation_id = slowly_calculate_sum_of_squares.start(bound=100)

# ... other code ...

# Gets the status/result of the remote invocation.
invocation = slowly_calculate_sum_of_squares.get(invocation_id)
print(invocation.status)  # e.g. PENDING, RUNNING, SUCCEEDED
print(invocation.result)  # 338350 (if available), or None (if still running)

# Perform downstream processing locally
half_of_sum_of_squares = invocation.result / 2
print(half_of_sum_of_squares)
```

So if you run `application/main.py` in the terminal...

```commandline
python application/main.py
```

... then:
- the code inside `application/main.py` will run locally;
- the invocation of `slowly_calculate_sum_of_squares` will run remotely in a cloud container
that is provisioned for the duration of the invocation.


### Authentication

For the `.start(...)` and `.get(...)` calls to work,
`application/main.py` must authenticate with the Multinode control plane.
For this, the following environment variables must be set:
- `MULTINODE_API_URL`: the URL of your Multinode control plane
- `MULTINODE_API_KEY`: the API key for your Multinode control plane

(These are the same credentials that you provide to the
[CLI tool](./registration.md#logging-in) when you run the `multinode login` command.
If you happen to be running `application/main.py` in Python environment where you have previously
run a `multinode login` command, then `application/main.py` will be able to authenticate
automatically without you having to explicitly set these environment variables.)


### Invoking functions from old project versions

By default, Multinode will create asynchronous tasks using the
[latest version](./registration.md#updating-project-function-definitions) of your project function code.
To use a historical version, you should pass the version ID to `get_deployed_function`.

```python
slowly_calculate_sum_of_squares = get_deployed_function(
    project_name="my_project",
    version_id="ver-12345",
    function_name="slowly_calculate_sum_of_squares"
)
```


## Nested function invocations

In certain situations, you may want to implement asynchronous tasks that trigger other asynchronous tasks.

For example, this is useful for implementing distributed computations where:
- the resource requirements change over different stages of the computation;
- the parallelism strategy is determined at runtime.

```python
# File: tasks/main.py

from multinode import Multinode

mn = Multinode()

@mn.function(cpu=4.0, memory="16 GiB")
def run_subtask(y):
    out =  # ... perform calculation
    return out

@mn.function(cpu=0.1, memory="1 GiB")
def run_full_task(x):
    y_1 =  # ... some code ...
    y_2 =  # ... some code ...
    
    # Starts two invocations of run_subtask, which run in a *separate containers*
    subtask_invocation_id_1 = run_subtask.start(y=y1)
    subtask_invocation_id_2 = run_subtask.start(y=y2)
    
    # ... more code ...
    
    # Gets the results of the two invocations of run_subtask (if available)
    subtask_result_1 = run_subtask.get(subtask_invocation_id_1).result
    subtask_result_2 = run_subtask.get(subtask_invocation_id_2).result

    out = # ... more code ...
    return out
```

```python
# File: application/main.py

from multinode import get_deployed_function

run_full_task = get_deployed_function(
    project_name="my_project",
    function_name="run_full_task"
)

# ... other code ...

# Starts an invocation of run_full_task, which runs in a *remote container*.
# (run_full_task will then create further containers for run_subtask...)
invocation_id = run_full_task.start(x=1)

# ... other code ...

# Gets the result of the invocation of run_full_task.
full_task_result = run_full_task.get(invocation_id).result

# ... other code ...
```


## Elementary function methods

A Multinode `Function` object (i.e. a function decorated with `mn.function()`, or
a function returned by `get_deployed_function`) has four elementary methods:
`.start`, `.get`, `.cancel` and `.list`.

We have already seen some of the functionality of `.start` and `.get` in the examples above,
but now, it is time to cover the full functionality of all these methods systematically.

### .start

The `.start` method on a Multinode function object creates a new invocation of the function, which runs on a
remote container that is dynamically provisioned for the duration of the invocation.

```python
invocation_id = my_function.start(1, 2)
```

The arguments to `.start` are the function arguments.
They can be passed:
- as args (e.g. `.start(1, 2)`)
- as kwargs (e.g. `.start(x=1, y=2)`)

The `.start` method returns the invocation ID - a string that uniquely identifies the invocation.

### .get

The `.get` method returns an `Invocation` object containing the status and result of a particular invocation,
plus further metadata.

```python
invocation = my_function.get(invocation_id)

print(invocation.status)  # e.g. RUNNING, SUCCEEDED
print(invocation.result)  # e.g. 42
```

The `Invocation` object has the following attributes:
- `.status`: an enum - either `PENDING`, `RUNNING`, `CANCELLING`,
    `SUCCEEDED`, `FAILED`, `CANCELLED` or `TIMED_OUT`.
- `.result`: the value returned by the function execution (if available), or `None` (if unavailable).
    If the function is a [generator](./definitions.md#exposing-intermediate-progress-updates)
    (i.e. it uses `yield` rather than `return`), then `.result` is the most recently yielded value.
- `.error`: an error message from the function execution (if the execution failed), or `None` (otherwise).
- `.terminated`: a boolean flag, indicating whether the invocation is terminated
- `.num_failed_attempts`: the number of failed executions so far

Note that it is possible for `invocation.terminated` to remain `False` for a short period of time after
`invocation.status` has reached `SUCCEEDED`, `FAILED`, `CANCELLED` or `TIMED_OUT`.
This is because it takes some time for a container to be deprovisioned from the cloud environment
after the container code has finished executing.

### .cancel

The `.cancel` method sends a signal to cancel an in-flight invocation.

```python
my_function.cancel(invocation_id)
```

An `InvocationCancelledError` will be thrown inside the function code, as demonstrated in
[this example](./definitions.md#graceful-handling-of-interruptions).

### .list

By default, the `.list` method returns an `InvocationIdsList` object, containing the IDs of the 50 most recent
invocations of the function.

```python
invocations_list = my_function.list()
print(invocations_list.invocation_ids)  # e.g. ["inv-12345", "inv-67890", ... ]
```

The following example shows how to list more than just the 50 most recent invocations, by passing offsets
to the `.list` method.

```python
offset = None

while True:
    invocations_list = my_function.list(offset=offset)
    print(invocations_list.invocations_ids)
    
    if invocations_list.offset is None:
        break
    
    offset = invocations_list.offset
```


## Further function methods

Although the `.start`, `.get`, `.cancel` and `.list` methods provide complete functionality,
the Multinode `Function` object has additional convenience methods that can help
simplify your code.

### .await_result

The `.await_result` method waits until an invocation reaches
a completed status (`SUCCEEDED`, `FAILED`, `CANCELLED` or `TIMED_OUT`),
then returns the result of the invocation if the status is `SUCCEEDED`, or else, throws an appropriate error.

```python
try:
    result = my_function.await_result(invocation_id)
except (InvocationFailedError, InvocationCancelledError, InvocationTimedOutError) as e:
    print (str(e))
```

Using `.await_result` is more convenient than repeatedly polling `.get`.

### .call_remote

The `.call_remote` method starts a remote invocation of the function, then awaits the result.

```python
try:
    result = my_function.call_remote(x=1, y=2)
except (InvocationFailedError, InvocationCancelledError, InvocationTimedOutError) as e:
    print(str(e))
```

Calling `.call_remote` is equivalent to calling `.start` followed by `.await_result`.

### .starmap

`.starmap` accepts an iterable of function argument tuples,
and creates a remote function invocation for each function argument tuple.
It returns a generator that yields the results as soon as they are ready,
in the same order as the argument tuples.

```python
# Will invoke my_function twice: with arguments 1, 2, and then with arguments 3, 4
arguments_list = [(1, 2), (3, 4)]

try:
    for result in my_function.starmap(arguments_list):
        print(result)
except (InvocationFailedError, InvocationCancelledError, InvocationTimedOutError) as e:
    print(str(e))
```

### .map

`.map` is similar to `.starmap`, except that it only works for functions that accept a single argument.
Whereas `.starmap` accepts an iterable of arguments tuples, `.map` accepts an iterable of arguments.

```python
# Will call single_arg_function twice: with argument 1, and then with argument 2.
arguments_list = [1, 2]

try:
    for result in single_arg_function.map(arguments_list):
        print(result)
except (InvocationFailedError, InvocationCancelledError, InvocationTimedOutError) as e:
    print (str(e))
```
