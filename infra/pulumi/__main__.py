"""An AWS Python Pulumi program"""

import pulumi
import pulumi_aws as aws
import pulumi_zenml as zenml
from pulumi import Config
from pulumi_zenml_infra.aws_resources import create_zenml_iam_resources
from pulumi_zenml_infra.zenml_service_connectors import create_service_connectors
from pulumi_zenml_infra.zenml_stack_components import (
    create_artifact_store,
    create_aws_step_functions_orchestrator,
    create_container_registry,
    create_local_docker_orchestrator,
)

config = Config()

stack_name = config.get("zenml_pulumi_stack") or "default-stack-name"
orchestrator = config.get("orchestrator") or "local"
zenml_stack_deployment = config.get("zenml_stack_deployment") or "default-deployment"

artifact_store_bucket = aws.s3.get_bucket("mlops-club-zenml-hackathon-artifact-store")
container_registry = aws.ecr.get_repository("zenml-hackathon-ecr-repo")

role, access_key_id, secret_access_key = create_zenml_iam_resources(
    artifact_store_bucket.bucket, container_registry.name
)

s3_connector, ecr_connector = create_service_connectors(
    stack_name=stack_name,
    role_arn=role.arn,
    access_key_id=access_key_id,
    secret_key=secret_access_key,
)

artifact_store = create_artifact_store(
    stack_name=stack_name,
    bucket_name=artifact_store_bucket.bucket,
    connector_id=s3_connector.id,
)

container_registry_component = create_container_registry(
    stack_name=stack_name,
    repository_url=container_registry.repository_url,
    repository_name=container_registry.name,
    connector_id=ecr_connector.id,
)

local_docker_orchestrator_component = create_local_docker_orchestrator()
step_functions_orchestrator = create_aws_step_functions_orchestrator()

zenml_stack = zenml.Stack(
    "zenml-local-docker-s3-ecr-stack",
    name="local-docker-s3-ecr",
    components={
        "artifact_store": artifact_store.id,
        "container_registry": container_registry_component.id,
        "orchestrator": local_docker_orchestrator_component.id,
    },
)

zenml_stack = zenml.Stack(
    "zenml-sfn-s3-ecr-stack",
    name="sfn-s3-ecr",
    components={
        "artifact_store": artifact_store.id,
        "container_registry": container_registry_component.id,
        "orchestrator": step_functions_orchestrator.id,
    },
)
