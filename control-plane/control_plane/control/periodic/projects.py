import logging

from control_plane.control.periodic.projects_deletion_helper import (
    classify_projects_for_possible_deletion,
)
from control_plane.data.data_store import DataStore
from control_plane.types.datatypes import InvocationStatus


class ProjectsLifecycleActions:
    """
    Control actions that progress Project objects through their lifecycle.
    These actions are executed periodically and in a single-threaded manner.
    """

    def __init__(self, data_store: DataStore):
        self._data_store = data_store

    def run_all(self, time: int) -> None:
        self.handle_projects_undergoing_deletion(time)

    def handle_projects_undergoing_deletion(self, time: int) -> None:
        projects = self._data_store.projects.list().projects

        running_invocations = self._data_store.invocations.list_all(
            statuses={InvocationStatus.RUNNING}
        )

        classification = classify_projects_for_possible_deletion(
            projects, running_invocations
        )

        for project in classification.projects_to_delete:
            self._data_store.projects.delete_with_cascade(
                project_name=project.project_name
            )

            logging.info(
                f"Deleted project ({project.project_name}) together with associated "
                f"project versions, functions, invocations and executions"
            )
