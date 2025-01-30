# 🎼 Create a Custom Orchestrator with ZenML

ZenML allows you to [create a custom orchestrator](https://docs.zenml.io/stack-components/orchestrators/custom), an essential component in any MLOps stack responsible for running your machine learning pipelines. This tutorial guides you through the process of creating an orchestrator that runs each step of a pipeline locally in a docker container.

## ❓Why would you need a custom orchestrator?

While ZenML comes built with standard integrations for well-known orchestrators like [Airflow](https://docs.zenml.io/stack-components/orchestrators/airflow), [Kubeflow](https://docs.zenml.io/stack-components/orchestrators/kubeflow), and even running [locally](https://docs.zenml.io/stack-components/orchestrators/local), your business might either want to orchestrate your ML workloads differently or slightly tweak the implementations of the standard orchestrators. In this case, this guide is useful, as it implements a relatively simple orchestrator.

The `BaseOrchestrator` abstracts away many of the ZenML-specific details from the actual implementation and exposes a simplified interface. This example is an end-to-end guide on creating a custom orchestrator using ZenML. Click [here](https://docs.zenml.io/stack-components/orchestrators/custom) to learn more about the custom orchestrator interface.

## 💻 Tutorial: Creating a custom orchestrator that runs each step in a docker container

### 📑 Prerequisites

To run this example, you need to have ZenML locally installed along with the necessary dependencies. You can do so by executing the following commands:

```shell
# clone the repository
git clone ...

# install the necessary dependencies
pip install -r requirements.txt
```

Also, you need to have a deployed ZenML server. You can do so by following the instructions [here](https://docs.zenml.io/getting-started/deploying-zenml) or by creating a free account on [ZenML Pro](https://cloud.zenml.io/). Assuming you have a ZenML server, you can run the following command to connect to it:

```shell
# connect to the ZenML server
zenml login <your-zenml-server-url>  # just use `zenml login` if you are using ZenML Pro
```

Please note that you also need to have Docker installed on your machine.

### 🚀 Registering a simple Custom Orchestrator: The `MyDockerOrchestrator`

In order to learn how to create a custom orchestrator, we will start with a simple one. This orchestrator will run each step of the pipeline in a docker container. This orchestrator is a good one to start because it can be coupled with a local artifact store, and
therefore can be run locally.

First, you need to initialize zenml at the root of the repository:

```shell
# initialize zenml at the root of the repository
zenml init
```

Then, you need to register the flavor of the orchestrator:

```shell
# register the flavor of the orchestrator
zenml orchestrator flavor register orchestrator.my_docker_orchestrator_flavor.LocalDockerOrchestratorFlavor
```

Then, you register your custom orchestrator using your registered flavor:

```shell
# register the custom orchestrator
zenml orchestrator register my_docker_orchestrator -f my_docker  
```

Note in this case, the registration of the orchestrator has no settings, as the orchestrator is not using any
config or settings. Your orchestrator might have more complicated config that you can set here.

### 📝 Registering and Setting the Stack

Next, you need to register a stack with your custom orchestrator and the default artifact store attached:

```shell
# register the stack
zenml stack register my_stack -o my_docker_orchestrator -a default
```

Finally, set the stack active. This means every pipeline that runs will use the custom orchestrator:

```shell
# set the stack active
zenml stack set my_stack
```

### 📝 Running a Pipeline and Testing the Orchestrator

This example ships with a simple pipeline that runs a `sklearn` pipeline. You can run it by executing the following command:

```shell
# run the pipeline
python run.py
```

By default, the pipeline is configured at runtime with the config file `configs/training_pipeline.yaml`.
When testing the orchestrator, you might want to check certain features like resource and orchestrator settings. You can do so by editing the config file:

```yaml
settings:
  docker:
    required_integrations:
      - sklearn
    requirements:
      - pyarrow
  orchestrator:
    some_setting: "some_value"

resources:
  cpu_count: 4
  memory: "4Gb"
```

## Start developing the AWS Step Orchestrator

Now that you have a working custom orchestrator, you can start developing the AWS Step Orchestrator.
In principle, the process is the same. You need to create a new flavor, register it, then register the orchestrator, and
finally register the stack with the orchestrator.

However, there are some nuances that you need to be aware of:

1. The AWS Step Orchestrator is a bit more complex than the Local Docker Orchestrator, as it requires a few more components to be set up. As the pipeline will run remotely, the stack needs to have an AWS artifact store and AWS container registry.
2. Ideally, the AWS Step Orchestrator will require to set up a [AWS Service Connector](https://docs.zenml.io/how-to/infrastructure-deployment/auth-management/aws-service-connector) in order to authenticate your local machine to AWS services. This is totally optional, but it is a good practice to do so.

Before running pipelines, ensure you have:
- An ECS cluster for running tasks
- A task definition that can run your containers
- An execution role with appropriate permissions
- VPC with subnets and security groups
- S3 bucket for artifacts
- ECR repository for container images

Here's a step-by-step guide on how to set up the AWS Step Orchestrator:

#### 1. Install Required Integrations and Register the AWS Step Orchestrator Custom Flavor
```bash
# Install AWS-related ZenML integrations
zenml integration install aws s3 -y
```

```bash
# Register the AWS Step Orchestrator Custom Flavor
zenml orchestrator flavor register orchestrator.my_aws_orchestrator_flavor.StepFunctionsOrchestratorFlavor
```

### 2. Set up AWS Service Connector
The AWS Service Connector helps authenticate with AWS services. You can set it up in multiple ways:

```bash
# Option 1: Auto-configure using your AWS CLI credentials
zenml service-connector register aws_auto --type aws --auto-configure

# Option 2: Configure with explicit credentials
zenml service-connector register aws_explicit --type aws \
    --auth-method secret-key \
    --region=<YOUR_REGION> \
    --access_key_id=<YOUR_ACCESS_KEY> \
    --secret_access_key=<YOUR_SECRET_KEY>

# Option 3: Configure with IAM role (recommended for production)
zenml service-connector register aws_role --type aws \
    --auth-method iam-role \
    --region=<YOUR_REGION> \
    --role_arn=<YOUR_ROLE_ARN> \
    --access_key_id=<YOUR_ACCESS_KEY> \
    --secret_access_key=<YOUR_SECRET_KEY>
```

The AWS Service Connector needs the following IAM permissions to work with Step Functions and ECS:

#### Step Functions Permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "states:CreateStateMachine",
                "states:DeleteStateMachine",
                "states:ListStateMachines",
                "states:StartExecution",
                "states:StopExecution",
                "states:ListExecutions",
                "states:DescribeExecution",
                "states:DescribeStateMachine",
                "states:GetExecutionHistory"
            ],
            "Resource": "*"
        }
    ]
}
```

#### ECS Permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ecs:RunTask",
                "ecs:StopTask",
                "ecs:DescribeTasks",
                "ecs:ListTasks",
                "ecs:DescribeTaskDefinition",
                "iam:PassRole"
                "iam:ListRoles"  # Needed for role validation
                "ecs:RegisterTaskDefinition",
                "ecs:DeregisterTaskDefinition",
                "ecs:ListTaskDefinitions",
                "logs:CreateLogGroup",
                "logs:PutLogEvents",
                "logs:CreateLogStream"
            ],
            "Resource": "*"
        }
    ]
}
```

#### Additional Required Permissions
If you're using the full stack with S3 and ECR:

- **S3 Permissions**:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<YOUR_BUCKET>",
                "arn:aws:s3:::<YOUR_BUCKET>/*"
            ]
        }
    ]
}
```

- **ECR Permissions**:
```json
{
    "Version": "2012-10-17",
    "Statement": [
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
                "ecr:PutImage"
            ],
            "Resource": "*"
        }
    ]
}
```

You can combine these permissions into a single IAM policy or create separate policies depending on your security requirements. For production environments, it's recommended to scope the `Resource` fields to specific ARNs rather than using `"*"`.

Additional Notes:

- Ensure Fargate launch type is enabled in ECS cluster
- Verify subnet IP addressing (Fargate requires non-default VPC)
- Confirm security groups allow outbound traffic


#### 3. Register Required Stack Components

```bash
# Register S3 artifact store
zenml artifact-store register s3_store \
    -f s3 \
    --path=s3://<YOUR_BUCKET_NAME> \
    --connector aws_auto

# Register ECR container registry
zenml container-registry register ecr_registry \
    -f aws \
    --uri=<ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com \
    --connector aws_auto

# Register AWS Step Functions orchestrator
zenml orchestrator register step_functions_orchestrator \
    -f aws_step_functions \
    --ecs_cluster_arn=<YOUR_ECS_CLUSTER_ARN> \
    --execution_role=<YOUR_EXECUTION_ROLE_ARN> \
    --task_role=<YOUR_TASK_ROLE_ARN> \
    --subnet_ids='["subnet-xxx", "subnet-yyy"]' \
    --security_group_ids='["sg-xxx"]' \
    --region=<YOUR_AWS_REGION>
```

#### 4. Create and Set Active Stack

```bash
# Register a stack with all components
zenml stack register aws_stack \
    -o step_functions_orchestrator \
    -a s3_store \
    -c ecr_registry

# Set the stack as active
zenml stack set aws_stack
```

#### 5. Run a Pipeline

```bash
# Run your pipeline
python run.py
```

The pipeline will now execute using AWS Step Functions, with:
- Steps running as ECS tasks
- Artifacts stored in S3
- Container images in ECR
- Workflow managed by Step Functions

## 📚 Learn More

For more information on creating a custom orchestrator in ZenML, follow this [guide](https://docs.zenml.io/stack-components/orchestrators/custom).