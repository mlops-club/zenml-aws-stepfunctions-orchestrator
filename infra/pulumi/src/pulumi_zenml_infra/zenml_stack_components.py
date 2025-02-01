from typing import Optional

import pulumi_zenml as zenml


def create_artifact_store(
    stack_name: str,
    bucket_name: str,
    connector_id: str,
) -> zenml.StackComponent:
    return zenml.StackComponent(
        "artifact-store",
        name=f"{stack_name}-artifact-store",
        type="artifact_store",
        flavor="s3",
        configuration={"path": f"s3://{bucket_name}"},
        connector_id=connector_id,
    )


def create_container_registry(
    stack_name: str,
    repository_url: str,
    repository_name: str,
    connector_id: str,
) -> zenml.StackComponent:
    return zenml.StackComponent(
        "container-registry",
        name=f"{stack_name}-container-registry",
        type="container_registry",
        flavor="aws",
        configuration={
            "uri": repository_url.split("/")[0],
            "default_repository": repository_name,
        },
        connector_id=connector_id,
    )


def create_local_docker_orchestrator() -> zenml.StackComponent:
    return zenml.StackComponent(
        "local-docker-orchestrator",
        name="local-docker",
        type="orchestrator",
        flavor="local_docker",
    )


def create_aws_step_functions_orchestrator() -> zenml.StackComponent:
    return zenml.StackComponent(
        "step-functions-orchestrator",
        name="step-functions",
        type="orchestrator",
        flavor="aws_step_functions",
        configuration={
            "name": "Amit",
        },
    )
