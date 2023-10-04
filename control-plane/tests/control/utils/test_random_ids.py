from control_plane.types.random_ids import generate_random_id


def test_generate_random_id() -> None:
    for _ in range(100):
        result = generate_random_id("prefix")
        assert result.startswith("prefix-")
        assert len(result) == len("prefix-") + 32
