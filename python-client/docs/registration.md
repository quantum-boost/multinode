# Function registration and lifecycle management: The CLI tool

Once you have [defined](./definitions.md) your project's asynchronous tasks as Python functions,
you must register these functions with the Multinode control plane before
you can [invoke](./invocations.md#start) them.

This section describes how to register a project's function definitions and manage
the lifecycle of a project using the `multinode` CLI.


## Authenticating with the control plane

### Logging in

To use the `multinode` CLI, you must first authenticate with the control plane
using the `multinode login` command.

```commandline
multinode login
```

You will be prompted to enter your Multinode control plane URL and API key,
which you should have noted down from when you [installed multinode](../../aws-infra/README.md)
into your AWS account.


### Logging out

The `multinode logout` command terminates an authenticated session.

```commandline
multinode logout
```


## Function lifecycle management

### Registering project function definitions

The `multinode deploy` command is used to register project function definitions with
the Multinode control plane. You must run `multinode deploy` before you can
[invoke](./invocations.md#start) your project functions.

```commandline
multinode deploy {CODE_PATH} --project-name={PROJECT_NAME}
```

Under the hood, this command:
- Builds a Docker image containing the function definitions in `{CODE_PATH}`.
- Uploads the Docker image to a private Docker repository in your AWS account.
- Registers the Docker image URI with the Multinode control plane,
associating it with the project name `{PROJECT_NAME}`.

When complete, the command returns a version ID for the project.

`{CODE_PATH}` is the folder containing the `main.py` file that contains the functions defining
your asynchronous tasks (i.e. the functions with the `@mn.function()` decorator).

For example, if your project folder structure is...

```
[CURRENT PATH]
└── tasks/
    ├── .env
    ├── requirements.txt
    ├── main.py
    └── submodule/
        ├── __init__.py
        ├── file_1.py
        └── file_2.py
```

... then `{CODE_PATH}` should be `tasks/`.


### Updating project function definitions

The `multinode upgrade` command is used to register a _new version_ of your project function definitions
with the Multinode control plane.

```commandline
multinode upgrade {CODE_PATH} --project-name={PROJECT_NAME}
```

When complete, this command returns a new version ID for the project.

In-flight function invocations will continue to run using the old version of the function definitions,
but future function invocations will use the new version of the code (unless a historical version ID
is [explicitly specified](./invocations.md#invoking-functions-from-old-project-versions)
in the codebase where the invocation is made).


### Deleting a project

The `multinode undeploy` command aborts all in-flight invocations associated with a project,
and deletes the project from the Multinode control plane.

```commandline
multinode undeploy --project-name={PROJECT_NAME}
```


### Listing existing projects

The `multinode list` command lists all projects currently registered with the control plane.

```commandline
multinode list
```


## Printing information about projects, functions and invocations

The `multinode describe` command prints information about projects, functions and invocations.
Exactly what is printed can be controlled by flags.


### Printing information about a project

```commandline
multinode describe --project-name={PROJECT_NAME}
```

This command prints:
- the IDs of all versions of the project
- the names of all functions associated with the latest version of the project

To see the names of functions associated with a _historical_ version of the project, use the following command.
```commandline
multinode describe --project-name={PROJECT_NAME} --version-id={VERSION_ID}
```

(The `--version-id={VERSION_ID}` flag can be used in a similar manner with the remaining
`multinode describe` commands, as well as with the `multinode logs` command, which is covered below.)


### Printing information about a function

```commandline
multinode describe --project-name={PROJECT_NAME} --function-name={FUNCTION_NAME}
```

This command prints:
- Information about the function - for example, its CPU and memory requirements, concurrency quota,
timeout limit and retry policy
- the IDs of recent invocations of this function


### Printing information about a function invocation

```commandline
multinode describe --project-name={PROJECT_NAME} --function-name={FUNCTION_NAME} \\
  --invocation-id={INVOCATION_ID}
```

This command prints:
- The status of the function invocation, and its creation time
- The ID, status and timestamps for every execution associated with this function invocation.

(An _invocation_ may have multiple _executions_ associated with it if it has been retried due to failures.)


### Viewing logs for a function execution

The `multinode logs` command prints the logs from a function execution.

```commandline
multinode logs --project-name={PROJECT_NAME} --function-name={FUNCTION_NAME} \\
  --invocation-id={INVOCATION_ID} --execution-id={EXECUTION_ID}
```

---

**Next:** [Function invocations](./invocations.md)
