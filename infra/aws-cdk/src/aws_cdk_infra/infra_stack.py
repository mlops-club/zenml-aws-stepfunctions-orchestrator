import aws_cdk as cdk
from aws_cdk import Stack  # Duration,; aws_sqs as sqs,
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_s3 as s3
from constructs import Construct


class ZenMLInfraStack(Stack):
    """
    Zen ML Infra Stack
    """

    def __init__(
        self,
        artifact_store_bucket_name: str,
        ecr_repo_name: str,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an S3 bucket for storing artifacts
        artifacts_bucket = s3.Bucket(
            self,
            id="artifacts-bucket",
            bucket_name=artifact_store_bucket_name,
            # @TODO: Avoid the below in production
            removal_policy=cdk.RemovalPolicy.DESTROY,  # Deletes the bucket when the stack is removed
            auto_delete_objects=True,  # Ensures all objects inside the bucket are deleted when the bucket is destroyed
        )

        # Create an ECR repository for storing Docker images
        ecr_repo = ecr.Repository(
            self,
            id="zenml-ecr-repo",
            repository_name=ecr_repo_name,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            empty_on_delete=True,
        )

        # Output the bucket name
        cdk.CfnOutput(self, "artifacts-bucket-name", value=artifacts_bucket.bucket_name)

        # Output the AWS Console URL dynamically using the region
        cdk.CfnOutput(
            self,
            "artifacts-bucket-aws-console-url",
            value=f"https://console.aws.amazon.com/s3/buckets/{artifacts_bucket.bucket_name}?region={self.region}",
        )

        # Output the ECR repository name
        cdk.CfnOutput(self, "ecr-repo-name", value=ecr_repo.repository_name)

        # Output the AWS Console URL dynamically using the region
        cdk.CfnOutput(
            self,
            "ecr-repo-aws-console-url",
            value=f"https://console.aws.amazon.com/ecr/repositories/{ecr_repo.repository_name}?region={self.region}",
            export_name="zenml-hackathon-ecr-repo-name",
        )
        
        cdk.CfnOutput(
            self,
            "ecr-repo-image-uri",
            value=f"{ecr_repo.repository_uri}:latest",  # assuming the image tag is 'latest'
            export_name="zenml-hackathon-ecr-repo-image-uri",
)
