from typing import NamedTuple

from control_plane.types.errortypes import OffsetIsInvalid


class ListOffset(NamedTuple):
    next_creation_time: int
    next_id: str

    def serialise(self) -> str:
        return f"{self.next_creation_time},{self.next_id}"

    @classmethod
    def deserialise(cls, offset_str: str) -> "ListOffset":
        try:
            index_of_first_comma = offset_str.index(",")
            next_creation_time = int(offset_str[:index_of_first_comma])
            next_id = offset_str[index_of_first_comma + 1 :]

            return ListOffset(next_creation_time=next_creation_time, next_id=next_id)

        except ValueError:
            # Raised if (i) no comma is found, (ii) the next creation time is not an int
            raise OffsetIsInvalid
