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

Running unit tests
```commandline
docker run --name postgres --env-file=environment/example-dev.env -p 5432:5432 -d postgres:15.4
poetry run pytest
```

Building Docker image
```commandline
poetry build --format wheel
docker build -t control-plane:latest .
```

Running loop and API in dev mode
```commandline
docker run --name postgres --env-file=environment/example-dev.env -p 5432:5432 -d postgres:15.4
docker run --name control-loop --env-file=environment/example-dev.env --network host -d control-plane:latest loop --provisioner=dev --create-tables --delete-tables
docker run --name control-api --env-file=environment/example-dev.env --network host -p 5000:5000 -d control-plane:latest api --provisioner=dev
```

Running loop and API with an external provisioner
```commandline
docker run --name postgres --env-file=environment/example-external.env -p 5432:5432 -d postgres:15.4
docker run --name control-loop --env-file=environment/example-external.env --network host -d control-plane:latest loop --provisioner=external --create-tables --delete-tables
docker run --name control-api --env-file=environment/example-external.env --network host -p 5000:5000 -d control-plane:latest api --provisioner=external
```

Calling this API
```commandline
curl -X GET http://localhost:5000/projects -H "Authorization: Bearer butterflyburger"
```

Auto-generating a schema and saving it to `../api-schemas/control-plane.json`. (NB this folder is gitignored)
```commandline
mkdir ../api-schemas
curl -X GET http://localhost:5000/openapi.json | jq '.' > ../api-schemas/control-plane.json
```

### Data structure

- Project = A codebase, containing definitions of functions
- Project version = A version of a project, corresponding to a particular docker image
- Function = A single function, defined inside a project version
- Invocation = A single attempt at calling a function
- Execution = A single attempt at executing a function invocation (NB a function invocation can be retried!)

So a project has multiple project versions, which each have multiple functions, which each have multiple invocations,
which each have multiple executions.
