import os

import pytest

from control_plane.entrypoints.utils.environment import (
    get_mandatory_environment_variable,
    get_optional_environment_variable_with_default,
)

DUMMY_KEY_1 = "key-1"
DUMMY_KEY_2 = "key-2"
DUMMY_VALUE_1 = "value-1"
DEFAULT_VALUE = "default"

os.environ[DUMMY_KEY_1] = DUMMY_VALUE_1


def test_get_mandatory_when_exists() -> None:
    assert get_mandatory_environment_variable(DUMMY_KEY_1) == DUMMY_VALUE_1


def test_get_mandatory_when_doesnt_exist() -> None:
    with pytest.raises(ValueError):
        get_mandatory_environment_variable(DUMMY_KEY_2)


def test_get_optional_with_default_when_exists() -> None:
    assert (
        get_optional_environment_variable_with_default(
            DUMMY_KEY_1, default=DEFAULT_VALUE
        )
        == DUMMY_VALUE_1
    )


def test_get_optional_with_default_when_doesnt_exist() -> None:
    assert (
        get_optional_environment_variable_with_default(
            DUMMY_KEY_2, default=DEFAULT_VALUE
        )
        == DEFAULT_VALUE
    )
