import os
from typing import Optional

from pydantic import BaseModel

from multinode.api_client import ApiClient, Configuration, DefaultApi
from multinode.constants import (
    CONFIG_FILE_PATH,
    CONTROL_PLANE_API_KEY_ENV,
    CONTROL_PLANE_API_URL_ENV,
    DEFAULT_API_URL,
)
from multinode.core.errors import MissingEnvironmentVariableError


class Config(BaseModel):
    api_url: str = DEFAULT_API_URL
    api_key: Optional[str] = None


def load_config_from_env() -> Config:
    config = Config()
    api_url = os.environ.get(CONTROL_PLANE_API_URL_ENV)
    if api_url is not None:
        config.api_url = api_url

    api_key = os.environ.get(CONTROL_PLANE_API_KEY_ENV)
    if api_key is not None:
        config.api_key = api_key

    return config


def load_config_from_file() -> Config:
    if CONFIG_FILE_PATH.exists():
        with CONFIG_FILE_PATH.open("r") as f:
            return Config.parse_raw(f.read())

    return Config()


def load_config_with_api_key_from_env_or_file() -> Config:
    config = load_config_from_env()
    if config.api_key is None:
        # Try loading it from file
        config = load_config_from_file()
        if config.api_key is None:
            # If it's still None, raise an exception
            raise MissingEnvironmentVariableError(
                f"{CONTROL_PLANE_API_KEY_ENV} environment variable is missing. "
                f"Cannot authenticate with the Multinode API."
            )

    return config


def save_config_to_file(config: Config) -> None:
    CONFIG_FILE_PATH.parent.mkdir(exist_ok=True)

    with CONFIG_FILE_PATH.open("w") as f:
        f.write(config.json(exclude_none=True, exclude_unset=True))


def create_control_plane_client_from_config(multinode_config: Config) -> DefaultApi:
    client_config = Configuration(
        host=multinode_config.api_url, access_token=multinode_config.api_key
    )
    client = ApiClient(client_config)
    return DefaultApi(client)
