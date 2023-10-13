from pathlib import Path
from typing import Optional

from pydantic import BaseModel

CONFIG_FILE_PATH = Path().home() / ".multinode" / "config.json"

DEFAULT_API_URL = "https://control.quantumboost-development.com"


class Config(BaseModel):
    api_url: str = DEFAULT_API_URL
    api_key: Optional[str] = None


def load_config_from_file() -> Config:
    if CONFIG_FILE_PATH.exists():
        with CONFIG_FILE_PATH.open("r") as f:
            return Config.parse_raw(f.read())

    return Config.parse_raw("{}")


def save_config_to_file(config: Config) -> None:
    CONFIG_FILE_PATH.parent.mkdir(exist_ok=True)

    with CONFIG_FILE_PATH.open("w") as f:
        f.write(config.json(exclude_none=True, exclude_unset=True))
