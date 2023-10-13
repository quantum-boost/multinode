from typing import Generic, List, TypeVar

from multinode_shared.api_client import FunctionSpec

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class Function(Generic[InputT, OutputT]):
    def __init__(self, fn_spec: FunctionSpec):
        self.fn_spec = fn_spec

    def start(self, input_data: InputT) -> str:
        raise NotImplementedError

    def get(self, invocation_id: str) -> OutputT:
        raise NotImplementedError

    def cancel(self, invocation_id: str) -> None:
        raise NotImplementedError

    def remove(self, invocation_id: str) -> None:
        raise NotImplementedError

    def list_ids(self) -> List[str]:
        raise NotImplementedError
