import os

import aws_cdk as cdk
from aws_cdk_infra.aws_batch_stack import AWSBatchStack
from aws_cdk_infra.infra_stack import ZenMLInfraStack
from aws_cdk_infra.aws_stepfunction_stack import AWSStepFunctionStack

app = cdk.App()


aws_env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# S3 & ECR Stack
infra_stack = ZenMLInfraStack(
    scope=app,
    construct_id="ZenMLInfraStack",
    artifact_store_bucket_name="mlops-club-zenml-hackathon-artifact-store",
    ecr_repo_name="zenml-hackathon-ecr-repo",
    env=aws_env,
)

# AWS Batch Stack
batch_stack = AWSBatchStack(app, "AWSBatchStack", env=aws_env)

# AWS Step Function Stack 
step_function_stack = AWSStepFunctionStack(app, "AWSStepFunctionStack", env=aws_env)

app.synth()
