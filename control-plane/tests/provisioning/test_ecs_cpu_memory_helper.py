from control_plane.provisioning.ecs_cpu_memory_helper import select_cpu_memory_units


def test_when_memory_limited() -> None:
    result = select_cpu_memory_units(desired_virtual_cpus=1.0, desired_memory_gbs=14.0)
    assert result.cpu_units == 2048
    assert result.memory_units == 16384


def test_when_cpu_limited() -> None:
    result = select_cpu_memory_units(desired_virtual_cpus=7.0, desired_memory_gbs=1.0)
    assert result.cpu_units == 8192
    assert result.memory_units == 16384


def test_when_request_is_too_large_for_any_available_size() -> None:
    # Defaults to the largest size.
    # Note that the API has input validation, so this can only happen due to rounding errors
    result = select_cpu_memory_units(
        desired_virtual_cpus=100.0, desired_memory_gbs=400.0
    )
    assert result.cpu_units == 16384
    assert result.memory_units == 65536
