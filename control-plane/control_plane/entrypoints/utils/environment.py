import os


def get_mandatory_environment_variable(name: str) -> str:
    if name in os.environ:
        return os.environ[name]
    else:
        raise ValueError(f"Environment variable {name} not found")


def get_optional_environment_variable_with_default(name: str, default: str) -> str:
    if name in os.environ:
        return os.environ[name]
    else:
        return default
