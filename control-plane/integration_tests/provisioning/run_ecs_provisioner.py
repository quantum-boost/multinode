import time

from control_plane.provisioning.ecs_provisioner import EcsProvisioner
from control_plane.types.datatypes import ResourceSpec, WorkerStatus, WorkerDetails, WorkerType

REGION = "eu-west-2"
CLUSTER_NAME = "Multinode"
SUBNET_IDS = ["subnet-07350b22534e0f5f8"]
SECURITY_GROUP_IDS = ["sg-0583d500dc1d94f2e"]
ASSIGN_PUBLIC_IP = True
TASK_ROLE_ARN = "arn:aws:iam::921216064263:role/MultinodeTaskRole"
EXECUTION_ROLE_ARN = "arn:aws:iam::921216064263:role/ecsTaskExecutionRole"
LOG_GROUP = "/ecs/multinode-dev"

CONTROL_PLANE_URL = "https://test.quantumboost-dev-environment.dev/"
API_KEY = "butterflyburger"

PROJECT_NAME = "project"
VERSION_ID = "version"
FUNCTION_NAME = "function"
INVOCATION_ID = "invocation"
EXECUTION_ID = "execution"

DOCKER_IMAGE = "nginx:latest"
RESOURCE_SPEC = ResourceSpec(virtual_cpus=0.25, memory_gbs=0.5, max_concurrency=1)


# Be careful with interrupting this!!! You may leave tasks running in our AWS account, which will cost money.


def main() -> None:
    provisioner = EcsProvisioner(
        region=REGION,
        cluster_name=CLUSTER_NAME,
        subnet_ids=SUBNET_IDS,
        security_group_ids=SECURITY_GROUP_IDS,
        assign_public_ip=ASSIGN_PUBLIC_IP,
        task_role_arn=TASK_ROLE_ARN,
        execution_role_arn=EXECUTION_ROLE_ARN,
        log_group=LOG_GROUP,
        control_api_url=CONTROL_PLANE_URL,
        api_key=API_KEY,
    )

    prepared_function_details = provisioner.prepare_function(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
    )

    print("Prepared function details:", prepared_function_details)

    worker_details = provisioner.provision_worker(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        resource_spec=RESOURCE_SPEC,
        prepared_function_details=prepared_function_details,
    )

    print("Worker details:", worker_details)

    status = provisioner.check_worker_status(worker_details=worker_details)
    print("Status:", status)

    assert status == WorkerStatus.RUNNING

    while True:
        logs_result = provisioner.get_worker_logs(worker_details=worker_details, max_lines=None, initial_offset=None)

        if len(logs_result.log_lines) == 0:
            print("Logs result:", logs_result)
            time.sleep(15)

        else:
            print("Logs result:", logs_result)
            break

    provisioner.send_termination_signal_to_worker(worker_details=worker_details)

    while status == WorkerStatus.RUNNING:
        status = provisioner.check_worker_status(worker_details=worker_details)
        print("Status:", status)
        time.sleep(15)

    assert status == WorkerStatus.TERMINATED

    # Also check that if a worker has already been erased from ECS, then we report it as terminated.
    nonexistent_worker_details = WorkerDetails(
        type=WorkerType.AWS_ECS,
        identifier=worker_details.identifier[:-1] + ("a" if worker_details.identifier[-1] != "a" else "b"),
        logs_identifier="",
    )

    status_of_nonexistent_worker = provisioner.check_worker_status(worker_details=nonexistent_worker_details)
    print("Status of non-existent worker:", status_of_nonexistent_worker)

    assert status_of_nonexistent_worker == WorkerStatus.TERMINATED


if __name__ == "__main__":
    main()
