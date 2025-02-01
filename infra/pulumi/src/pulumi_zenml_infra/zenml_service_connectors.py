from typing import Tuple

import pulumi_zenml as zenml
from pulumi import Output


def create_s3_connector(
    stack_name: str,
    role_arn: Output[str],
    access_key_id: Output[str],
    secret_key: Output[str],
    region: str = "us-west-2",
) -> zenml.ServiceConnector:
    return zenml.ServiceConnector(
        "s3-connector",
        name=f"{stack_name}-s3-connector",
        type="aws",
        auth_method="iam-role",
        resource_type="s3-bucket",
        configuration={
            "role_arn": role_arn,
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_key,
            "region": region,
        },
    )


def create_ecr_connector(
    stack_name: str,
    role_arn: Output[str],
    access_key_id: Output[str],
    secret_key: Output[str],
    region: str = "us-west-2",
) -> zenml.ServiceConnector:
    return zenml.ServiceConnector(
        "ecr-connector",
        name=f"{stack_name}-ecr-connector",
        type="aws",
        auth_method="iam-role",
        resource_type="docker-registry",
        configuration={
            "role_arn": role_arn,
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_key,
            "region": region,
        },
    )


def create_service_connectors(
    stack_name: str,
    role_arn: Output[str],
    access_key_id: Output[str],
    secret_key: Output[str],
    region: str = "us-west-2",
) -> Tuple[zenml.ServiceConnector, zenml.ServiceConnector]:
    s3_connector = create_s3_connector(
        stack_name, role_arn, access_key_id, secret_key, region
    )
    ecr_connector = create_ecr_connector(
        stack_name, role_arn, access_key_id, secret_key, region
    )
    return s3_connector, ecr_connector
