from control_plane.data.executions import ExecutionsTable
from control_plane.data.functions import FunctionsTable
from control_plane.data.invocations import InvocationsTable
from control_plane.data.projects import ProjectsTable
from control_plane.data.versions import VersionsTable


class DataStore:
    @property
    def projects(self) -> ProjectsTable:
        raise NotImplementedError

    @property
    def project_versions(self) -> VersionsTable:
        raise NotImplementedError

    @property
    def functions(self) -> FunctionsTable:
        raise NotImplementedError

    @property
    def invocations(self) -> InvocationsTable:
        raise NotImplementedError

    @property
    def executions(self) -> ExecutionsTable:
        raise NotImplementedError
