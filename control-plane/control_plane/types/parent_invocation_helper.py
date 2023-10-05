# Sometimes, information is passed in query parameters, so our API code has to do some parsing
from typing import Optional

from control_plane.types.api_errors import (
    ParentFunctionNameIsMissing,
    ParentInvocationIdIsMissing,
)
from control_plane.types.datatypes import ParentInvocationDefinition


def parse_parent_invocation_definition(
    parent_function_name: Optional[str], parent_invocation_id: Optional[str]
) -> Optional[ParentInvocationDefinition]:
    if parent_function_name is not None and parent_invocation_id is not None:
        return ParentInvocationDefinition(
            function_name=parent_function_name, invocation_id=parent_invocation_id
        )
    elif parent_function_name is None and parent_invocation_id is not None:
        raise ParentFunctionNameIsMissing
    elif parent_function_name is not None and parent_invocation_id is None:
        raise ParentInvocationIdIsMissing
    else:
        return None
