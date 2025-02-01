import boto3

batch_client = boto3.client('batch')

response = batch_client.register_job_definition(
    jobDefinitionName='zenml-fargate-job-def-from-python',
    type='container',
    containerProperties={
        'image': '847068433460.dkr.ecr.us-west-2.amazonaws.com/zenml-hackathon-ecr-repo:simple_pipeline-orchestrator',  # Replace with your ECR image URI
        'executionRoleArn': 'arn:aws:iam::847068433460:role/zenml-hackathon-batch-job-role',  # Replace with the ECS Task Execution Role ARN
        'networkConfiguration': {
            'assignPublicIp': 'ENABLED'
        },
        'fargatePlatformConfiguration': {
            'platformVersion': 'LATEST'
        },
        'resourceRequirements': [
            {'type': 'VCPU', 'value': '1'},  # Correct way to specify vCPUs
            {'type': 'MEMORY', 'value': '2048'}  # Correct way to specify Memory (in MiB)
        ],
        # 'jobRoleArn': '<your-ecs-task-role>',  # Replace with your ECS Task Role ARN
    },
    platformCapabilities=['FARGATE']
)

print(f"Job Definition Registered: {response['jobDefinitionArn']}")



response = batch_client.submit_job(
    jobName='zenml-fargate-job-run-python',
    jobQueue='zenml-fargate-queue-manual',  # Replace with your AWS Batch job queue name
    jobDefinition='zenml-test',
    containerOverrides={
        'command': ["python", "-m", "zenml.entrypoints.entrypoint", "--entrypoint_config_source", "zenml.entrypoints.step_entrypoint_configuration.StepEntrypointConfiguration", "--deployment_id", "17a11a59-bad0-4377-aa31-ea8b6e85c152", "--step_name", "step_1"],  # Replace with actual command
        # 'memory': 2048,
        # 'vcpus': 1,
        # 'environment': [
        #     {'name': 'ENV_VAR', 'value': 'my_value'}  # Add environment variables if needed
        # ]
    }
)

print(f"Job Submitted: {response['jobId']}")

