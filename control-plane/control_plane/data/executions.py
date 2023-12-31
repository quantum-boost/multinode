from typing import Any, Optional, Tuple

import psycopg2

from control_plane.data.error_helpers import raise_error_if_invocation_does_not_exist
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import (
    ExecutionAlreadyExists,
    ExecutionDoesNotExist,
    ExecutionHasAlreadyFinished,
    ExecutionHasAlreadyStarted,
    ExecutionHasNotFinished,
    ExecutionHasNotStarted,
    ExecutionIsAlreadyTerminated,
    InvocationDoesNotExist,
)
from control_plane.types.datatypes import (
    ExecutionInfo,
    ExecutionOutcome,
    ExecutionsListForInvocation,
    ExecutionSpec,
    ExecutionSummary,
    FunctionStatus,
    PreparedFunctionDetails,
    ResourceSpec,
    WorkerDetails,
    WorkerStatus,
)


class ExecutionsTable:
    def __init__(self, pool: SqlConnectionPool) -> None:
        self._pool = pool

    def _create_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS executions (
                  project_name TEXT NOT NULL,
                  version_id TEXT NOT NULL,
                  function_name TEXT NOT NULL,
                  invocation_id TEXT NOT NULL,
                  execution_id TEXT NOT NULL,
                  worker_status TEXT NOT NULL,
                  worker_details TEXT,
                  termination_signal_time INTEGER,
                  outcome TEXT,
                  output TEXT,
                  error_message TEXT,
                  creation_time INTEGER NOT NULL,
                  last_update_time INTEGER NOT NULL,
                  execution_start_time INTEGER,
                  execution_finish_time INTEGER,
                  PRIMARY KEY (project_name, version_id, function_name, invocation_id, execution_id),
                  FOREIGN KEY (project_name, version_id, function_name, invocation_id)
                    REFERENCES invocations(project_name, version_id, function_name, invocation_id)
                    ON DELETE CASCADE ON UPDATE CASCADE
                );
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS executions_ordering
                ON executions(project_name ASC, version_id ASC, function_name ASC, invocation_id ASC,
                              creation_time DESC, execution_id ASC);
                """
            )

    def _delete_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                DROP INDEX IF EXISTS executions_ordering;
                """
            )

            cursor.execute(
                """
                DROP TABLE IF EXISTS executions;
                """
            )

    def create(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        worker_status: WorkerStatus,
        worker_details: Optional[WorkerDetails],
        termination_signal_time: Optional[int],
        outcome: Optional[ExecutionOutcome],
        output: Optional[str],
        error_message: Optional[str],
        creation_time: int,
        last_update_time: int,
        execution_start_time: Optional[int],
        execution_finish_time: Optional[int],
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionAlreadyExists:
        """
        raise_error_if_invocation_does_not_exist(
            project_name, version_id, function_name, invocation_id, self._pool
        )

        with self._pool.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    INSERT INTO executions
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    [
                        project_name,
                        version_id,
                        function_name,
                        invocation_id,
                        execution_id,
                        worker_status.value,
                        worker_details.model_dump_json()
                        if worker_details is not None
                        else None,
                        termination_signal_time,
                        outcome.value if outcome is not None else None,
                        output,
                        error_message,
                        creation_time,
                        last_update_time,
                        execution_start_time,
                        execution_finish_time,
                    ],
                )
            except psycopg2.errors.UniqueViolation:
                raise ExecutionAlreadyExists
            except psycopg2.errors.ForeignKeyViolation:
                # Handle rare race condition in case a project deletion is happening concurrently.
                raise InvocationDoesNotExist

    def update(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        update_time: int,
        new_worker_status: Optional[WorkerStatus] = None,
        new_worker_details: Optional[WorkerDetails] = None,
        new_termination_signal_time: Optional[int] = None,
        new_outcome: Optional[ExecutionOutcome] = None,
        new_output: Optional[str] = None,
        new_error_message: Optional[str] = None,
        new_execution_start_time: Optional[int] = None,
        new_execution_finish_time: Optional[int] = None,
        should_already_have_started: bool = False,
        should_not_already_have_started: bool = False,
        should_already_have_finished: bool = False,
        should_not_already_have_finished: bool = False,
        should_not_already_be_terminated: bool = False,
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        :raises ExecutionHasNotStarted:
        :raises ExecutionHasAlreadyStarted:
        :raises ExecutionHasNotFinished:
        :raises ExecutionHasAlreadyFinished:
        :raises ExecutionIsAlreadyTerminated:
        """
        raise_error_if_invocation_does_not_exist(
            project_name, version_id, function_name, invocation_id, self._pool
        )

        with self._pool.cursor() as cursor:
            set_statement_clauses: list[str] = []
            set_statement_args: list[Any] = []

            set_statement_clauses.append("last_update_time = %s")
            set_statement_args.append(update_time)

            if new_worker_status is not None:
                set_statement_clauses.append("worker_status = %s")
                set_statement_args.append(new_worker_status.value)

            if new_worker_details is not None:
                set_statement_clauses.append("worker_details = %s")
                set_statement_args.append(new_worker_details.model_dump_json())

            if new_termination_signal_time is not None:
                set_statement_clauses.append("termination_signal_time = %s")
                set_statement_args.append(new_termination_signal_time)

            if new_outcome is not None:
                set_statement_clauses.append("outcome = %s")
                set_statement_args.append(new_outcome.value)

            if new_output is not None:
                set_statement_clauses.append("output = %s")
                set_statement_args.append(new_output)

            if new_error_message is not None:
                set_statement_clauses.append("error_message = %s")
                set_statement_args.append(new_error_message)

            if new_execution_start_time is not None:
                set_statement_clauses.append("execution_start_time = %s")
                set_statement_args.append(new_execution_start_time)

            if new_execution_finish_time is not None:
                set_statement_clauses.append("execution_finish_time = %s")
                set_statement_args.append(new_execution_finish_time)

            set_statement = "SET " + ", ".join(set_statement_clauses)

            extra_where_statement_clauses: list[str] = []
            extra_where_statement_args: list[Any] = []

            if should_already_have_started:
                extra_where_statement_clauses.append("execution_start_time IS NOT NULL")

            if should_not_already_have_started:
                extra_where_statement_clauses.append("execution_start_time IS NULL")

            if should_already_have_finished:
                extra_where_statement_clauses.append(
                    "execution_finish_time IS NOT NULL"
                )

            if should_not_already_have_finished:
                extra_where_statement_clauses.append("execution_finish_time IS NULL")

            if should_not_already_be_terminated:
                extra_where_statement_clauses.append("worker_status != %s")
                extra_where_statement_args.append(WorkerStatus.TERMINATED.value)

            if len(extra_where_statement_clauses) > 0:
                extra_where_statement = " AND " + " AND ".join(
                    extra_where_statement_clauses
                )
            else:
                extra_where_statement = ""

            cursor.execute(
                f"""
                UPDATE executions
                {set_statement}
                WHERE project_name = %s
                  AND version_id = %s
                  AND function_name = %s
                  AND invocation_id = %s
                  AND execution_id = %s
                  {extra_where_statement};
                """,
                set_statement_args
                + [
                    project_name,
                    version_id,
                    function_name,
                    invocation_id,
                    execution_id,
                ]
                + extra_where_statement_args,
            )

            if cursor.rowcount == 0:
                # We need to diagnose the reason why no row matched the WHERE statement.
                # It could be that the execution_id doesn't exist.
                # But it could also be that we have violated conditions of the update.
                if (
                    should_already_have_started
                    or should_not_already_have_started
                    or should_already_have_finished
                    or should_not_already_have_finished
                    or should_not_already_be_terminated
                ):
                    cursor.execute(
                        f"""
                        SELECT execution_start_time, execution_finish_time, worker_status
                        FROM executions
                        WHERE project_name = %s
                          AND version_id = %s
                          AND function_name = %s
                          AND invocation_id = %s
                          AND execution_id = %s;
                        """,
                        [
                            project_name,
                            version_id,
                            function_name,
                            invocation_id,
                            execution_id,
                        ],
                    )

                    row = cursor.fetchone()

                    # Check row is not None to protect against rare race conditions
                    # due to concurrent deletions.
                    if row is not None:
                        has_already_started = row[0] is not None
                        has_already_finished = row[1] is not None
                        is_terminated = WorkerStatus(row[2]) == WorkerStatus.TERMINATED

                        if not has_already_started and should_already_have_started:
                            raise ExecutionHasNotStarted

                        if has_already_started and should_not_already_have_started:
                            raise ExecutionHasAlreadyStarted

                        if not has_already_finished and should_already_have_finished:
                            raise ExecutionHasNotFinished

                        if has_already_finished and should_not_already_have_finished:
                            raise ExecutionHasAlreadyFinished

                        if is_terminated and should_not_already_be_terminated:
                            raise ExecutionIsAlreadyTerminated

                # Default reason why update didn't happen:
                raise ExecutionDoesNotExist

    def get(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        """
        raise_error_if_invocation_does_not_exist(
            project_name, version_id, function_name, invocation_id, self._pool
        )

        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  executions.project_name,
                  executions.version_id,
                  executions.function_name,
                  executions.invocation_id,
                  executions.execution_id,
                  invocations.input,
                  invocations.cancellation_request_time,
                  functions.resource_spec,
                  functions.execution_spec,
                  functions.function_status,
                  functions.prepared_function_details,
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
                  invocations.creation_time
                FROM functions
                INNER JOIN invocations USING (project_name, version_id, function_name)
                INNER JOIN executions USING (project_name, version_id, function_name, invocation_id)
                WHERE executions.project_name = %s
                  AND executions.version_id = %s
                  AND executions.function_name = %s
                  AND executions.invocation_id = %s
                  AND executions.execution_id = %s;
                """,
                [project_name, version_id, function_name, invocation_id, execution_id],
            )

            row = cursor.fetchone()

            if row is None:
                raise ExecutionDoesNotExist

            return _construct_execution_info_from_row(row)

    def list_all(self, *, worker_statuses: set[WorkerStatus]) -> list[ExecutionInfo]:
        """
        The worker_statuses argument must be populated. It is unwise to call this method with the TERMINATED status,
        since that is likely to return a very large number of results.
        """
        if len(worker_statuses) == 0:
            return []

        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  executions.project_name,
                  executions.version_id,
                  executions.function_name,
                  executions.invocation_id,
                  executions.execution_id,
                  invocations.input,
                  invocations.cancellation_request_time,
                  functions.resource_spec,
                  functions.execution_spec,
                  functions.function_status,
                  functions.prepared_function_details,
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
                  invocations.creation_time
                FROM functions
                INNER JOIN invocations
                  USING (project_name, version_id, function_name)
                INNER JOIN executions
                  USING (project_name, version_id, function_name, invocation_id)
                WHERE executions.worker_status IN %s;
                """,
                [tuple(status.value for status in worker_statuses)],
            )

            rows = cursor.fetchall()

            return [_construct_execution_info_from_row(row) for row in rows]


def _construct_execution_info_from_row(row: Tuple[Any, ...]) -> ExecutionInfo:
    return ExecutionInfo(
        project_name=row[0],
        version_id=row[1],
        function_name=row[2],
        invocation_id=row[3],
        execution_id=row[4],
        input=row[5],
        cancellation_request_time=row[6],
        resource_spec=ResourceSpec.model_validate_json(row[7]),
        execution_spec=ExecutionSpec.model_validate_json(row[8]),
        function_status=FunctionStatus(row[9]),
        prepared_function_details=(
            PreparedFunctionDetails.model_validate_json(row[10])
            if row[10] is not None
            else None
        ),
        worker_status=WorkerStatus(row[11]),
        worker_details=(
            WorkerDetails.model_validate_json(row[12]) if row[12] is not None else None
        ),
        termination_signal_time=row[13],
        outcome=(ExecutionOutcome(row[14]) if row[14] is not None else None),
        output=row[15],
        error_message=row[16],
        creation_time=row[17],
        last_update_time=row[18],
        execution_start_time=row[19],
        execution_finish_time=row[20],
        invocation_creation_time=row[21],
    )
