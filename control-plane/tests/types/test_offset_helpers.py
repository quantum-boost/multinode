import pytest

from control_plane.types.api_errors import OffsetIsInvalid
from control_plane.types.offset_helpers import ListOffset


def test_serialise_then_deserialise() -> None:
    offset = ListOffset(next_creation_time=1234, next_id="inv,1-2.3/4|5!6")
    offset_string = offset.serialise()
    reconstructed_offset = ListOffset.deserialise(offset_string)
    assert offset == reconstructed_offset


def test_deserialise_when_missing_comma() -> None:
    offset_string = "abcde"
    with pytest.raises(OffsetIsInvalid):
        ListOffset.deserialise(offset_string)


def test_deserialise_when_creation_time_is_not_an_int() -> None:
    offset_string = "abc,de"
    with pytest.raises(OffsetIsInvalid):
        ListOffset.deserialise(offset_string)
