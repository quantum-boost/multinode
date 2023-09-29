from control_plane.data.executions import ExecutionsTable
from control_plane.data.functions import FunctionsTable
from control_plane.data.invocations import InvocationsTable
from control_plane.data.projects import ProjectsTable
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.data.versions import VersionsTable


class DataStore:
    def __init__(self, pool: SqlConnectionPool) -> None:
        self._projects = ProjectsTable(pool)
        self._project_versions = VersionsTable(pool)
        self._functions = FunctionsTable(pool)

    def create_tables(self) -> None:
        # Must create in forward order, due to foreign keys
        self._projects._create_table()
        self._project_versions._create_table()
        self._functions._create_table()

    def delete_tables(self) -> None:
        # Must delete in reverse order, due to foreign keys
        self._functions._delete_table()
        self._project_versions._delete_table()
        self._projects._delete_table()

    @property
    def projects(self) -> ProjectsTable:
        return self._projects

    @property
    def project_versions(self) -> VersionsTable:
        return self._project_versions

    @property
    def functions(self) -> FunctionsTable:
        return self._functions

    @property
    def invocations(self) -> InvocationsTable:
        raise NotImplementedError

    @property
    def executions(self) -> ExecutionsTable:
        raise NotImplementedError
