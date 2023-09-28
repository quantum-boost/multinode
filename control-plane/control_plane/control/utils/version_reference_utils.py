from control_plane.data.data_store import DataStore
from control_plane.types.datatypes import VersionReference, VersionReferenceType


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
