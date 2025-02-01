import json

import pulumi
import pulumi_aws as aws


def create_zenml_iam_resources(artifact_store_bucket_name: str, ecr_repo_name: str):
    # Create IAM user first so we can use its ARN in the role trust policy
    user = aws.iam.User("zenml-local-orchestrator-stack-service-user", path="/service/")

    # Modified assume role policy to allow both AWS services AND our IAM user
    assume_role_policy = pulumi.Output.all(user_arn=user.arn).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": [
                                "ecs-tasks.amazonaws.com",
                                "batch.amazonaws.com",
                            ]
                        },
                        "Action": "sts:AssumeRole",
                    },
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": args[
                                "user_arn"
                            ]  # Allow our IAM user to assume this role
                        },
                        "Action": "sts:AssumeRole",
                    },
                ],
            }
        )
    )

    # Create IAM role with updated trust policy
    role = aws.iam.Role(
        "zenml-service-role",
        assume_role_policy=assume_role_policy,
        description="Role for ZenML to access AWS services",
    )

    # S3 bucket policy - updated to include ListAllMyBuckets
    s3_policy = aws.iam.RolePolicy(
        "zenml-s3-policy",
        role=role.id,
        policy=pulumi.Output.all(bucket_name=artifact_store_bucket_name).apply(
            lambda args: json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "s3:ListAllMyBuckets",
                            "Resource": "*",
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "s3:Put*",
                                "s3:Get*",
                                "s3:List*",
                                "s3:Delete*",
                                "s3:Head*",
                            ],
                            "Resource": [
                                f"arn:aws:s3:::{args['bucket_name']}",
                                f"arn:aws:s3:::{args['bucket_name']}/*",
                            ],
                        },
                    ],
                }
            )
        ),
    )

    # ECR policy - updated to include broader repository access
    ecr_policy = aws.iam.RolePolicy(
        "zenml-ecr-policy",
        role=role.id,
        policy=pulumi.Output.all(repo_name=ecr_repo_name).apply(
            lambda args: json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["ecr:GetAuthorizationToken"],
                            "Resource": "*",
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "ecr:GetAuthorizationToken",
                                "ecr:BatchCheckLayerAvailability",
                                "ecr:GetDownloadUrlForLayer",
                                "ecr:GetRepositoryPolicy",
                                "ecr:DescribeRepositories",
                                "ecr:ListImages",
                                "ecr:DescribeImages",
                                "ecr:BatchGetImage",
                                "ecr:InitiateLayerUpload",
                                "ecr:UploadLayerPart",
                                "ecr:CompleteLayerUpload",
                                "ecr:PutImage",
                            ],
                            "Resource": [
                                f"arn:aws:ecr:*:*:repository/{args['repo_name']}",
                                "arn:aws:ecr:*:*:repository/*",  # Allow listing all repos
                            ],
                        },
                    ],
                }
            )
        ),
    )

    access_key = aws.iam.AccessKey(
        "zenml-local-orchestrator-stack-service-key", user=user.name
    )

    return role, access_key.id, access_key.secret
