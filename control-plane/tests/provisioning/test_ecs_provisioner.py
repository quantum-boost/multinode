from control_plane.provisioning.ecs_provisioner import select_cpu_memory_units


def test_select_cpu_memory_units_when_memory_bound() -> None:
    chosen_units = select_cpu_memory_units(desired_virtual_cpus=0.5, desired_memory_gbs=20.0)
    assert chosen_units.cpu_units == 8192
    assert chosen_units.memory_units == 32768


def test_select_cpu_memory_units_when_cpu_bound() -> None:
    chosen_units = select_cpu_memory_units(desired_virtual_cpus=7.0, desired_memory_gbs=2.0)
    assert chosen_units.cpu_units == 8192
    assert chosen_units.memory_units == 16384
