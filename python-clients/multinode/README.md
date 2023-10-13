# Client for defining and creating multinode deployments

## Running CLI
You can run any of the CLI commands using the `poetry run multinode` prefix. For example:
```commandline
poetry run multinode --help
```

### Deploying your first project
To deploy your first project in the development mode, first run:
```commandline
poetry run multinode login
```

You will be asked for an API key.

Once successfully logged in, create a `test.py` file with the following content:
```python
from multinode import Multinode

mn = Multinode()


@mn.job()
def a():
    print("Hello, Multinode A!")


@mn.job()
def b():
    print("Hello, Multinode B!")
```

and run

```commandline
poetry run multinode deploy test.py --project-name my-first-project
```
