import inspect
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from typing import List

from multinode.core.multinode import Multinode


def import_multinode_object_from_dir(directory: Path) -> Multinode:
    # Assume PYTHONPATH is at the root of the project dir
    sys.path.append(str(directory))
    main_filepath = directory / "main.py"

    module = _import_python_module_from_file(main_filepath)
    multinode_objects = _extract_multinode_objects_from_module(module)

    if len(multinode_objects) == 0:
        raise ImportError(f"Could not find a Multinode object in {main_filepath}.")
    if len(multinode_objects) > 1:
        raise ImportError(
            f"Found more than one Multinode object in {main_filepath}. "
            f"Only one is allowed.",
        )

    return multinode_objects[0]


def _extract_multinode_objects_from_module(module: ModuleType) -> List[Multinode]:
    mn_objects: List[Multinode] = []
    for _, obj in inspect.getmembers(module):
        if isinstance(obj, Multinode):
            mn_objects.append(obj)

    return mn_objects


def _import_python_module_from_file(filepath: Path) -> ModuleType:
    """
    Dynamically imports a Python file.

    Keep in mind that importing a file also executes all the code inside.

    :return: `ModuleType` object.
    :raises ImportError: if the file cannot be imported as a Python module.
    """
    module_name = inspect.getmodulename(filepath)
    if module_name is None:
        raise ImportError(f"Could not import {filepath} as a python module.")

    spec = spec_from_file_location(module_name, filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not import {filepath} as a python module.")

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[module_name] = module
    return module
