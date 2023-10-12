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

Running loop and API with the ECS provisioner.
(Example IAM permissions in iam_permissions/iam_policy.json.
The AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables are not required if using an IAM role.)
```commandline
docker run --name postgres --env-file=environment/example-ecs.env -p 5432:5432 -d postgres:15.4
docker run --name control-loop --env-file=environment/example-ecs.env --network host -d control-plane:latest loop --provisioner=ecs --create-tables --delete-tables
docker run --name control-api --env-file=environment/example-ecs.env --network host -p 5000:5000 -d control-plane:latest api --provisioner=ecs
```

Calling this API
```commandline
curl -X GET http://localhost:5000/projects -H "Authorization: Bearer butterflyburger"
```

Auto-generating an OpenAPI schema and error type documentation.
(NB the ../api-schemas folder is gitignored)
```commandline
mkdir ../api-schemas
poetry run generate-schema --schema-output ../api-schemas/control-plane.json --error-types-output ../api-schemas/control-plane-errors.json
```

### Data structure

- Project = A codebase, containing definitions of functions
- Project version = A version of a project, corresponding to a particular docker image
- Function = A single function, defined inside a project version
- Invocation = A single attempt at calling a function
- Execution = A single attempt at executing a function invocation (NB a function invocation can be retried!)

So a project has multiple project versions, which each have multiple functions, which each have multiple invocations,
which each have multiple executions.
