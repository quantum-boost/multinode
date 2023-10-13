from typing import NoReturn, Optional

import click
from multinode_shared.api_client import (
    ApiException,
    DefaultApi,
    FunctionInfoForVersion,
    InvocationInfo,
    InvocationInfoForFunction,
    InvocationsListForFunction,
    InvocationStatus,
    ProjectInfo,
    VersionDefinition,
    VersionInfo,
)

from multinode import Multinode

MAX_LIST_RESULTS = 10  # TODO make the CLI tool dynamic so it fetches more on scroll
MAX_LIST_COUNT = 50


def cli_fail(ctx: click.Context, message: str) -> NoReturn:
    click.secho(message, fg="red")
    ctx.exit(1)


def create_project(api_client: DefaultApi, project_name: str) -> Optional[ProjectInfo]:
    try:
        project = api_client.create_project(project_name)
    except ApiException as e:
        if e.status == 409:
            return None
        else:
            raise e

    return project


def create_project_version(
    api_client: DefaultApi,
    project_name: str,
    multinode_obj: Multinode,
) -> VersionInfo:
    functions = [function.fn_spec for function in multinode_obj.functions.values()]
    # TODO nginx is just a placeholder, provide actual docker image
    version_def = VersionDefinition(
        default_docker_image="nginx:latest", functions=functions
    )
    version = api_client.create_project_version(
        project_name=project_name, version_definition=version_def
    )
    return version


def describe_project(
    api_client: DefaultApi,
    project: ProjectInfo,
) -> None:
    deletion_line = (
        "\tTHE PROJECT HAS BEEN MARKED FOR DELETION\n"
        if project.deletion_request_time is not None
        else ""
    )
    click.echo(
        click.style(f"{project.project_name} details:\n", bold=True)
        + f"{deletion_line}"
        + f"\tcreation time: {project.creation_time}\n"
    )

    versions = api_client.list_project_versions(project.project_name).versions
    version_lines = [click.style(f"{project.project_name} versions:\n", bold=True)]
    for v in versions:
        version_lines.append(f"\t{v.version_id}\n")
    click.echo("".join(version_lines))


def describe_version(
    api_client: DefaultApi,
    project: ProjectInfo,
    version: VersionInfo,
    latest: bool = False,
) -> None:
    latest_suffix = ""
    if latest:
        latest_suffix = " (latest)"

    click.echo(
        click.style(f"{version.version_id}{latest_suffix} details:\n", bold=True)
        + f"\tcreation time: {version.creation_time}\n"
    )

    click.secho(f"{version.version_id}{latest_suffix} functions:", bold=True)
    for f in version.functions:
        invocations = api_client.list_invocations(
            project.project_name,
            version.version_id,
            f.function_name,
            max_results=MAX_LIST_COUNT,
        )
        _echo_function_details(f, invocations, line_prefix="\t")


def describe_function(
    api_client: DefaultApi,
    project: ProjectInfo,
    version: VersionInfo,
    function: FunctionInfoForVersion,
) -> None:
    invocations = api_client.list_invocations(
        project.project_name,
        version.version_id,
        function.function_name,
        max_results=MAX_LIST_COUNT,
    )
    _echo_function_details(function, invocations, bold_name=True)
    click.secho(f"{function.function_name} invocations:", bold=True)

    if len(invocations.invocations) == 0:
        click.echo(f"\t{function.function_name} has no invocations so far")

    invocations_to_list = invocations.invocations[:MAX_LIST_RESULTS]
    for i in invocations_to_list:
        _echo_basic_invocation_details(i, line_prefix="\t")
        still_running = (
            "YES" if i.invocation_status == InvocationStatus.RUNNING else "NO"
        )
        click.echo(f"\t\tstill running?: {still_running}\n")


def describe_invocation(
    api_client: DefaultApi,
    project: ProjectInfo,
    version: VersionInfo,
    function: FunctionInfoForVersion,
    invocation: InvocationInfo,
) -> None:
    # TODO let's implement when invocations can actually via the CLI tool
    raise NotImplementedError


def _echo_function_details(
    function: FunctionInfoForVersion,
    invocations_list: InvocationsListForFunction,
    line_prefix: str = "",
    bold_name: bool = False,
) -> None:
    n_total_invocations = len(invocations_list.invocations)

    count_suffix = ""
    if invocations_list.next_offset is not None:
        count_suffix = "+"

    click.echo(
        click.style(f"{line_prefix}{function.function_name}:\n", bold=bold_name)
        + f"{line_prefix}\tdocker image: {function.docker_image}\n"
        + f"{line_prefix}\tcpus: {function.resource_spec.virtual_cpus}\n"
        + f"{line_prefix}\tmemory: {function.resource_spec.memory_gbs} GiB\n"
        + f"{line_prefix}\tmax concurrency: {function.resource_spec.max_concurrency}\n"
        + f"{line_prefix}\tmax retries: {function.execution_spec.max_retries}\n"
        + f"{line_prefix}\ttimeout: {function.execution_spec.timeout_seconds}s\n"
        + f"{line_prefix}\ttotal invocations: {n_total_invocations}{count_suffix}\n"
    )


def _echo_basic_invocation_details(
    invocation: InvocationInfoForFunction,
    line_prefix: str = "",
) -> None:
    parent_invocation_line = ""
    if invocation.parent_invocation is not None:
        parent_inv = invocation.parent_invocation
        parent_invocation_line = (
            f"{line_prefix}\tparent invocation: "
            f"{parent_inv.invocation_id} ({parent_inv.function_name})\n"
        )

    status = (
        "IN-FLIGHT"
        if invocation.invocation_status == InvocationStatus.RUNNING
        else "TERMINATED"
    )

    click.echo(
        f"{line_prefix}{invocation.invocation_id}:\n"
        f"{line_prefix}\tcreation time: {invocation.creation_time}\n"
        f"{line_prefix}\tstatus: {status}"
        f"{parent_invocation_line}"
    )
