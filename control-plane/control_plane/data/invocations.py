from typing import Any, NamedTuple, Optional, Tuple

import psycopg2

from control_plane.data.error_helpers import raise_error_if_function_does_not_exist
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import (
    FunctionDoesNotExist,
    InvocationAlreadyExists,
    InvocationDoesNotExist,
    InvocationIsAlreadyTerminated,
    ParentInvocationDoesNotExist,
)
from control_plane.types.datatypes import (
    ExecutionOutcome,
    ExecutionSpec,
    ExecutionSummary,
    FunctionStatus,
    InvocationInfo,
    InvocationInfoForFunction,
    InvocationsListForFunction,
    InvocationStatus,
    ParentInvocationDefinition,
    ParentInvocationInfo,
    PreparedFunctionDetails,
    ResourceSpec,
    WorkerDetails,
    WorkerStatus,
)
from control_plane.types.offset_helpers import ListOffset


class InvocationsTable:
    def __init__(self, pool: SqlConnectionPool) -> None:
        self._pool = pool

    def _create_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS invocations (
                  project_name TEXT NOT NULL,
                  version_id TEXT NOT NULL,
                  function_name TEXT NOT NULL,
                  invocation_id TEXT NOT NULL,
                  parent_function_name TEXT,
                  parent_invocation_id TEXT,
                  input TEXT NOT NULL,
                  cancellation_request_time INTEGER,
                  invocation_status TEXT NOT NULL,
                  creation_time INTEGER NOT NULL,
                  last_update_time INTEGER NOT NULL,
                  PRIMARY KEY (project_name, version_id, function_name, invocation_id),
                  FOREIGN KEY (project_name, version_id, function_name)
                    REFERENCES functions(project_name, version_id, function_name)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                  FOREIGN KEY (project_name, version_id, parent_function_name, parent_invocation_id)
                    REFERENCES invocations(project_name, version_id, function_name, invocation_id)
                    ON DELETE CASCADE ON UPDATE CASCADE
                );
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS invocations_ordering
                ON invocations(project_name ASC, version_id ASC, function_name ASC,
                               creation_time DESC, invocation_id ASC);
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS invocations_parents
                ON invocations(parent_function_name, parent_invocation_id);
                """
            )

    def _delete_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                DROP INDEX IF EXISTS invocations_parents;
                """
            )

            cursor.execute(
                """
                DROP INDEX IF EXISTS invocations_ordering;
                """
            )

            cursor.execute(
                """
                DROP TABLE IF EXISTS invocations;
                """
            )

    def create(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        parent_invocation: Optional[ParentInvocationDefinition],
        input: str,
        cancellation_request_time: Optional[int],
        invocation_status: InvocationStatus,
        creation_time: int,
        last_update_time: int,
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationAlreadyExists:
        :raises ParentInvocationDoesNotExist:
        """
        raise_error_if_function_does_not_exist(
            project_name, version_id, function_name, self._pool
        )

        with self._pool.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    INSERT INTO invocations
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    [
                        project_name,
                        version_id,
                        function_name,
                        invocation_id,
                        parent_invocation.function_name
                        if parent_invocation is not None
                        else None,
                        parent_invocation.invocation_id
                        if parent_invocation is not None
                        else None,
                        input,
                        cancellation_request_time,
                        invocation_status.value,
                        creation_time,
                        last_update_time,
                    ],
                )
            except psycopg2.errors.UniqueViolation:
                raise InvocationAlreadyExists
            except psycopg2.errors.ForeignKeyViolation as ex:
                if "parent" in str(ex):
                    raise ParentInvocationDoesNotExist
                else:
                    # Handle rare race condition in case a project deletion is happening concurrently.
                    raise FunctionDoesNotExist

    def update(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        update_time: int,
        new_cancellation_request_time: Optional[int] = None,
        new_invocation_status: Optional[InvocationStatus] = None,
        should_not_already_be_terminated: bool = False,
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises InvocationIsAlreadyTerminated:
        """
        raise_error_if_function_does_not_exist(
            project_name, version_id, function_name, self._pool
        )

        with self._pool.cursor() as cursor:
            set_statement_clauses: list[str] = []
            set_statement_args: list[Any] = []

            set_statement_clauses.append("last_update_time = %s")
            set_statement_args.append(update_time)

            if new_cancellation_request_time is not None:
                set_statement_clauses.append("cancellation_request_time = %s")
                set_statement_args.append(new_cancellation_request_time)

            if new_invocation_status is not None:
                set_statement_clauses.append("invocation_status = %s")
                set_statement_args.append(new_invocation_status.value)

            set_statement = "SET " + ", ".join(set_statement_clauses)

            extra_where_statement_clauses: list[str] = []
            extra_where_statement_args: list[Any] = []

            if should_not_already_be_terminated:
                extra_where_statement_clauses.append("invocation_status != %s")
                extra_where_statement_args.append(InvocationStatus.TERMINATED.value)

            if len(extra_where_statement_clauses) > 0:
                extra_where_statement = " AND " + " AND ".join(
                    extra_where_statement_clauses
                )
            else:
                extra_where_statement = ""

            cursor.execute(
                f"""
                UPDATE invocations
                {set_statement}
                WHERE project_name = %s AND version_id = %s AND function_name = %s AND invocation_id = %s
                {extra_where_statement};
                """,
                set_statement_args
                + [project_name, version_id, function_name, invocation_id]
                + extra_where_statement_args,
            )

            if cursor.rowcount == 0:
                # We need to diagnose the reason why no row matched the WHERE statement.
                # It could be that the invocation_id doesn't exist.
                # But it could also be that we have violated a condition of the update.
                if should_not_already_be_terminated:
                    cursor.execute(
                        f"""
                        SELECT invocation_status
                        FROM invocations
                        WHERE project_name = %s
                          AND version_id = %s
                          AND function_name = %s
                          AND invocation_id = %s;
                        """,
                        [project_name, version_id, function_name, invocation_id],
                    )

                    row = cursor.fetchone()

                    # Check row is not None to protect against rare race conditions
                    # due to concurrent deletions.
                    if row is not None:
                        is_terminated = (
                            InvocationStatus(row[0]) == InvocationStatus.TERMINATED
                        )

                        if is_terminated and should_not_already_be_terminated:
                            raise InvocationIsAlreadyTerminated

                raise InvocationDoesNotExist

    def get(
        self,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
    ) -> InvocationInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        """
        raise_error_if_function_does_not_exist(
            project_name, version_id, function_name, self._pool
        )

        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  main_invocations.project_name,
                  main_invocations.version_id,
                  main_invocations.function_name,
                  main_invocations.invocation_id,
                  main_invocations.parent_function_name,
                  main_invocations.parent_invocation_id,
                  parent_invocations.cancellation_request_time,
                  parent_invocations.invocation_status,
                  parent_invocations.creation_time,
                  parent_invocations.last_update_time,
                  functions.resource_spec,
                  functions.execution_spec,
                  functions.function_status,
                  functions.prepared_function_details,
                  main_invocations.input,
                  main_invocations.cancellation_request_time,
                  main_invocations.invocation_status,
                  main_invocations.creation_time,
                  main_invocations.last_update_time
                FROM functions
                INNER JOIN invocations main_invocations USING (project_name, version_id, function_name)
                LEFT JOIN invocations parent_invocations
                  ON main_invocations.project_name = parent_invocations.project_name
                  AND main_invocations.version_id = parent_invocations.version_id
                  AND main_invocations.parent_function_name = parent_invocations.function_name
                  AND main_invocations.parent_invocation_id = parent_invocations.invocation_id
                WHERE main_invocations.project_name = %s
                  AND main_invocations.version_id = %s
                  AND main_invocations.function_name = %s
                  AND main_invocations.invocation_id = %s;
                """,
                [project_name, version_id, function_name, invocation_id],
            )

            row = cursor.fetchone()

            if row is None:
                raise InvocationDoesNotExist

            invocation = _construct_invocation_info_from_row(row)

            # Must also fetch the executions for this invocation.
            # If we want to be clever, we can merge this query and the previous query into a single query
            # using a LEFT JOIN; however, that will make the code harder to understand.
            cursor.execute(
                """
                SELECT
                  execution_id,
                  worker_status,
                  worker_details,
                  termination_signal_time,
                  outcome,
                  output,
                  error_message,
                  creation_time,
                  last_update_time,
                  execution_start_time,
                  execution_finish_time
                FROM executions
                WHERE project_name = %s AND version_id = %s AND function_name = %s AND invocation_id = %s;
                """,
                [project_name, version_id, function_name, invocation_id],
            )

            rows = cursor.fetchall()
            for row in rows:
                invocation.executions.append(_construct_execution_summary_from_row(row))

            invocation.executions.sort(key=(lambda exe: exe.creation_time))

            return invocation

    def list_for_function(
        self,
        project_name: str,
        version_id: str,
        function_name: str,
        max_results: Optional[int] = None,
        initial_offset: Optional[str] = None,
        status: Optional[InvocationStatus] = None,
        parent_invocation: Optional[ParentInvocationDefinition] = None,
    ) -> InvocationsListForFunction:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises OffsetIsInvalid:
        """
        raise_error_if_function_does_not_exist(
            project_name, version_id, function_name, self._pool
        )

        if initial_offset is not None:
            initial_offset_obj = ListOffset.deserialise(initial_offset)
        else:
            initial_offset_obj = None

        with self._pool.cursor() as cursor:
            extra_where_statement_clauses: list[str] = []
            extra_where_statement_args: list[Any] = []

            if status is not None:
                extra_where_statement_clauses.append("invocation_status = %s")
                extra_where_statement_args.append(status.value)

            if parent_invocation is not None:
                extra_where_statement_clauses.append("parent_function_name = %s")
                extra_where_statement_args.append(parent_invocation.function_name)

                extra_where_statement_clauses.append("parent_invocation_id = %s")
                extra_where_statement_args.append(parent_invocation.invocation_id)

            if initial_offset_obj is not None:
                extra_where_statement_clauses.append(
                    "(creation_time < %s OR (creation_time = %s AND invocation_id >= %s))"
                )
                extra_where_statement_args.extend(
                    [
                        initial_offset_obj.next_creation_time,
                        initial_offset_obj.next_creation_time,
                        initial_offset_obj.next_id,
                    ]
                )

            if len(extra_where_statement_clauses) > 0:
                extra_where_statement = " AND " + " AND ".join(
                    extra_where_statement_clauses
                )
            else:
                extra_where_statement = ""

            if max_results is not None:
                limit_statement = "LIMIT %s"
                limit_statement_args = [
                    max_results + 1
                ]  # plus 1 because we need the offset for the next page
            else:
                limit_statement = ""
                limit_statement_args = []

            # Again, write out the ORDER BY in a way that explicitly encourages use of the index,
            # even though, in reality, the aim is to sort by creation_time, using invocation_id as a tie-break.
            cursor.execute(
                f"""
                SELECT
                  invocation_id,
                  parent_function_name,
                  parent_invocation_id,
                  cancellation_request_time,
                  invocation_status,
                  creation_time,
                  last_update_time
                FROM invocations
                WHERE project_name = %s AND version_id = %s AND function_name = %s
                {extra_where_statement}
                ORDER BY project_name ASC,
                  version_id ASC,
                  function_name ASC,
                  creation_time DESC,
                  invocation_id ASC
                {limit_statement}
                """,
                [project_name, version_id, function_name]
                + extra_where_statement_args
                + limit_statement_args,
            )

            rows = cursor.fetchall()

            if max_results is not None:
                rows_to_return = rows[:max_results]
            else:
                rows_to_return = rows

            invocations: list[InvocationInfoForFunction] = []

            for row in rows_to_return:
                if row[1] is not None:
                    parent_invocation_def = ParentInvocationDefinition(
                        function_name=row[1], invocation_id=row[2]
                    )
                else:
                    parent_invocation_def = None

                invocations.append(
                    InvocationInfoForFunction(
                        invocation_id=row[0],
                        parent_invocation=parent_invocation_def,
                        cancellation_request_time=row[3],
                        invocation_status=InvocationStatus(row[4]),
                        creation_time=row[5],
                        last_update_time=row[6],
                    )
                )

            if max_results is not None and len(rows) > max_results:
                next_row = rows[max_results]
                next_offset = ListOffset(
                    next_creation_time=next_row[5], next_id=next_row[0]
                ).serialise()
            else:
                next_offset = None

            return InvocationsListForFunction(
                project_name=project_name,
                version_id=version_id,
                function_name=function_name,
                invocations=invocations,
                next_offset=next_offset,
            )

    def list_all(self, *, statuses: set[InvocationStatus]) -> list[InvocationInfo]:
        """
        The statuses argument must be populated. It is unwise to call this method with the TERMINATED status,
        since that is likely to return a very large number of results.
        """
        if len(statuses) == 0:
            return []

        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  main_invocations.project_name,
                  main_invocations.version_id,
                  main_invocations.function_name,
                  main_invocations.invocation_id,
                  main_invocations.parent_function_name,
                  main_invocations.parent_invocation_id,
                  parent_invocations.cancellation_request_time,
                  parent_invocations.invocation_status,
                  parent_invocations.creation_time,
                  parent_invocations.last_update_time,
                  functions.resource_spec,
                  functions.execution_spec,
                  functions.function_status,
                  functions.prepared_function_details,
                  main_invocations.input,
                  main_invocations.cancellation_request_time,
                  main_invocations.invocation_status,
                  main_invocations.creation_time,
                  main_invocations.last_update_time
                FROM functions
                INNER JOIN invocations main_invocations USING (project_name, version_id, function_name)
                LEFT JOIN invocations parent_invocations
                  ON main_invocations.project_name = parent_invocations.project_name
                  AND main_invocations.version_id = parent_invocations.version_id
                  AND main_invocations.parent_function_name = parent_invocations.function_name
                  AND main_invocations.parent_invocation_id = parent_invocations.invocation_id
                WHERE main_invocations.invocation_status IN %s;
                """,
                [tuple(status.value for status in statuses)],
            )

            rows = cursor.fetchall()
            invocations: list[InvocationInfo] = [
                _construct_invocation_info_from_row(row) for row in rows
            ]

            # Must also fetch the executions associated with any of these invocations.
            # Again, we can potentially consider merging this query and the previous one using a LEFT JOIN,
            # although that may make the code harder to understand.
            cursor.execute(
                """
                SELECT
                  executions.execution_id,
                  executions.worker_status,
                  executions.worker_details,
                  executions.termination_signal_time,
                  executions.outcome,
                  executions.output,
                  executions.error_message,
                  executions.creation_time,
                  executions.last_update_time,
                  executions.execution_start_time,
                  executions.execution_finish_time,
                  invocations.project_name,
                  invocations.version_id,
                  invocations.function_name,
                  invocations.invocation_id,
                  invocations.invocation_status
                FROM invocations
                INNER JOIN executions USING (project_name, version_id, function_name, invocation_id)
                WHERE invocations.invocation_status IN %s;
                """,
                [tuple(status.value for status in statuses)],
            )

            rows = cursor.fetchall()
            executions: list[ExecutionSummaryWithInvocationIdentifier] = [
                _construct_execution_summary_with_invocation_identifier_from_row(row)
                for row in rows
            ]

            _attach_execution_summaries_to_invocation_infos(invocations, executions)

            return invocations


def _construct_invocation_info_from_row(row: Tuple[Any, ...]) -> InvocationInfo:
    if row[4] is not None:
        parent_invocation = ParentInvocationInfo(
            function_name=row[4],
            invocation_id=row[5],
            cancellation_request_time=row[6],
            invocation_status=InvocationStatus(row[7]),
            creation_time=row[8],
            last_update_time=row[9],
        )
    else:
        parent_invocation = None

    return InvocationInfo(
        project_name=row[0],
        version_id=row[1],
        function_name=row[2],
        invocation_id=row[3],
        parent_invocation=parent_invocation,
        resource_spec=ResourceSpec.model_validate_json(row[10]),
        execution_spec=ExecutionSpec.model_validate_json(row[11]),
        function_status=FunctionStatus(row[12]),
        prepared_function_details=(
            PreparedFunctionDetails.model_validate_json(row[13])
            if row[13] is not None
            else None
        ),
        input=row[14],
        cancellation_request_time=row[15],
        invocation_status=InvocationStatus(row[16]),
        creation_time=row[17],
        last_update_time=row[18],
        executions=[],  # will populate later
    )


def _construct_execution_summary_from_row(row: Tuple[Any, ...]) -> ExecutionSummary:
    return ExecutionSummary(
        execution_id=row[0],
        worker_status=WorkerStatus(row[1]),
        worker_details=(
            WorkerDetails.model_validate_json(row[2]) if row[2] is not None else None
        ),
        termination_signal_time=row[3],
        outcome=(ExecutionOutcome(row[4]) if row[4] is not None else None),
        output=row[5],
        error_message=row[6],
        creation_time=row[7],
        last_update_time=row[8],
        execution_start_time=row[9],
        execution_finish_time=row[10],
    )


class InvocationUniqueIdentifier(NamedTuple):
    project_name: str
    version_id: str
    function_name: str
    invocation_id: str


class ExecutionSummaryWithInvocationIdentifier(NamedTuple):
    execution_summary: ExecutionSummary
    invocation_identifier: InvocationUniqueIdentifier


def _construct_execution_summary_with_invocation_identifier_from_row(
    row: Tuple[Any, ...]
) -> ExecutionSummaryWithInvocationIdentifier:
    return ExecutionSummaryWithInvocationIdentifier(
        execution_summary=_construct_execution_summary_from_row(row),
        invocation_identifier=InvocationUniqueIdentifier(
            project_name=row[11],
            version_id=row[12],
            function_name=row[13],
            invocation_id=row[14],
        ),
    )


def _attach_execution_summaries_to_invocation_infos(
    invocation_infos: list[InvocationInfo],
    execution_summaries: list[ExecutionSummaryWithInvocationIdentifier],
) -> None:
    invocations_dict: dict[InvocationUniqueIdentifier, InvocationInfo] = dict()
    for invocation in invocation_infos:
        invocation_identifier = InvocationUniqueIdentifier(
            project_name=invocation.project_name,
            version_id=invocation.version_id,
            function_name=invocation.function_name,
            invocation_id=invocation.invocation_id,
        )
        invocations_dict[invocation_identifier] = invocation

    for execution in execution_summaries:
        associated_invocation = invocations_dict.get(execution.invocation_identifier)

        # Check associated_invocation exists, since the execution summaries are loaded in a separate query from
        # the query that originally loaded the invocation infos. A few extra executions could have been added
        # to the database in the meantime!
        if associated_invocation is not None:
            associated_invocation.executions.append(execution.execution_summary)
