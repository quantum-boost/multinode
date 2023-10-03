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

Running ECS integration tests
(requires `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `DEFAULT_AWS_REGION` environment variables,
where the access key has the permissions in iam_permissions/ci/ecs_provisioning.json).
```commandline
poetry run ecs-test
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

Running loop and API using ECS provisioner (requires the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables
to refer to an access key with the permissions in iam_permissions/ci/ecs_provisioning.json;
can omit these environment variables if using an IAM role).
```commandline
docker run --name postgres --env-file=environment/example-ecs.env -p 5432:5432 -d postgres:15.4
docker run --name control-loop --env-file=environment/example-ecs.env --network host -d control-plane:latest loop --provisioner=ecs --create-tables --delete-tables
docker run --name control-api --env-file=environment/example-ecs.env --network host -p 5000:5000 -d control-plane:latest api --provisioner=ecs
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
