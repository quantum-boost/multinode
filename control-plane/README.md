### Development

Installing dependencies
```commandline
poetry install
```

Running type checker
```commandline
poetry run mypy .
```

Running formatter
```commandline
poetry run black .
```

Running unit tests (requires Postgres)
```commandline
docker run --name postgres -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=db -p 5432:5432 -d postgres:15.4
poetry run pytest
```

Building Docker image
```commandline
poetry build --format wheel
docker build -t control-plane:latest .
```

Running Docker image
```commandline
docker run -d -p 5000:5000 control-plane:latest api
```


### Data structure

- Project = A codebase, containing definitions of functions
- Project version = A version of a project, corresponding to a particular docker image
- Function = A single function, defined inside a project version
- Invocation = A single attempt at calling a function
- Execution = A single attempt at executing a function invocation (NB a function invocation can be retried!)

So a project has multiple project versions, which each have multiple functions, which each have multiple invocations,
which each have multiple executions.
