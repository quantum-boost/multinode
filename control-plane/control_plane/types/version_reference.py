from enum import Enum
from typing import NamedTuple, Optional

from control_plane.data.data_store import DataStore


class VersionReferenceType(Enum):
    NAMED = "NAMED"
    LATEST = "LATEST"


class VersionReference(NamedTuple):
    type: VersionReferenceType
    named_version_id: Optional[str]


def resolve_version_reference(
    project_name: str, version_ref: VersionReference, data_store: DataStore
) -> str:
    if version_ref.type == VersionReferenceType.NAMED:
        assert version_ref.named_version_id is not None
        return version_ref.named_version_id
    elif version_ref.type == VersionReferenceType.LATEST:
        return data_store.project_versions.get_id_of_latest_version(
            project_name=project_name
        )
    else:
        raise ValueError


LATEST_STR = "latest"


def parse_version_reference(version_id_str: str) -> VersionReference:
    if version_id_str.lower() == LATEST_STR:
        return VersionReference(type=VersionReferenceType.LATEST, named_version_id=None)
    else:
        return VersionReference(
            type=VersionReferenceType.NAMED, named_version_id=version_id_str
        )
