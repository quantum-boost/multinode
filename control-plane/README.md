Installing dependencies
```commandline
poetry install
```

Running type checker
```commandline
poetry run mypy .
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
