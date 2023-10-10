from enum import Enum
from typing import NamedTuple, Optional


class VersionReferenceType(Enum):
    NAMED = "NAMED"
    LATEST = "LATEST"


class VersionReference(NamedTuple):
    type: VersionReferenceType
    named_version_id: Optional[str]


LATEST_STR = "latest"


def parse_version_reference(version_id_str: str) -> VersionReference:
    if version_id_str.lower() == LATEST_STR:
        return VersionReference(type=VersionReferenceType.LATEST, named_version_id=None)
    else:
        return VersionReference(
            type=VersionReferenceType.NAMED, named_version_id=version_id_str
        )
