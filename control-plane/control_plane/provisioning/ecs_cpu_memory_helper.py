from typing import NamedTuple


class EcsCpuMemoryUnits(NamedTuple):
    cpu_units: int  # NB 1 virtual CPU is 1024 of these CPU units
    memory_units: int  # 1 memory GB is 1024 of these memory units


# The ordering of this list in important - we pick the *first* item that is big enough for the user's requirements.
# This is not a complete list. But it's enough to get going with.
ALLOWED_CPU_MEMORY_OPTIONS: list[EcsCpuMemoryUnits] = [
    EcsCpuMemoryUnits(cpu_units=256, memory_units=512),
    EcsCpuMemoryUnits(cpu_units=256, memory_units=1024),
    EcsCpuMemoryUnits(cpu_units=256, memory_units=2048),
    EcsCpuMemoryUnits(cpu_units=512, memory_units=1024),
    EcsCpuMemoryUnits(cpu_units=512, memory_units=2048),
    EcsCpuMemoryUnits(cpu_units=512, memory_units=4096),
    EcsCpuMemoryUnits(cpu_units=1024, memory_units=2048),
    EcsCpuMemoryUnits(cpu_units=1024, memory_units=4096),
    EcsCpuMemoryUnits(cpu_units=1024, memory_units=8192),
    EcsCpuMemoryUnits(cpu_units=2048, memory_units=4096),
    EcsCpuMemoryUnits(cpu_units=2048, memory_units=8192),
    EcsCpuMemoryUnits(cpu_units=2048, memory_units=16384),
    EcsCpuMemoryUnits(cpu_units=4096, memory_units=8192),
    EcsCpuMemoryUnits(cpu_units=4096, memory_units=16384),
    EcsCpuMemoryUnits(cpu_units=8192, memory_units=16384),
    EcsCpuMemoryUnits(cpu_units=8192, memory_units=32768),
    EcsCpuMemoryUnits(cpu_units=16384, memory_units=32768),
    EcsCpuMemoryUnits(cpu_units=16384, memory_units=65536),
]

EPSILON = 1.0e-5


def select_cpu_memory_units(
    desired_virtual_cpus: float, desired_memory_gbs: float
) -> EcsCpuMemoryUnits:
    for option in ALLOWED_CPU_MEMORY_OPTIONS:
        enough_cpu = (
            option.cpu_units / (1024.0 * desired_virtual_cpus)
        ) > 1.0 - EPSILON
        enough_memory = (
            option.memory_units / (1024.0 * desired_memory_gbs)
        ) > 1.0 - EPSILON
        if enough_cpu and enough_memory:
            return option

    # In case of rounding errors at the top end, return the most expensive option.
    return ALLOWED_CPU_MEMORY_OPTIONS[-1]
