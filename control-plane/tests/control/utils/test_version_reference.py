from control_plane.types.version_reference import (
    VersionReferenceType,
    parse_version_reference,
)


def test_parse_with_latest() -> None:
    original_string = "latest"
    version_ref = parse_version_reference(original_string)
    assert version_ref.type == VersionReferenceType.LATEST


def test_parse_with_named_version_id() -> None:
    original_string = "ver-1234"
    version_ref = parse_version_reference(original_string)
    assert version_ref.type == VersionReferenceType.NAMED
    assert version_ref.named_version_id == original_string
