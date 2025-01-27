"""Improved AWS Step Functions Orchestrator with Parallel Execution and Safety Checks"""

import hashlib
import os
import json
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    Optional,
    Tuple,
    Type,
    cast,
    List,
)

from collections import deque
from typing import List, Dict, Set
from zenml.models import PipelineDeploymentResponse
from uuid import UUID
from botocore.exceptions import ClientError

import boto3

from zenml.config.base_settings import BaseSettings
from zenml.constants import (
    METADATA_ORCHESTRATOR_LOGS_URL,
    METADATA_ORCHESTRATOR_RUN_ID,
    METADATA_ORCHESTRATOR_URL,
)
from zenml.enums import StackComponentType
from zenml.logger import get_logger
from zenml.metadata.metadata_types import MetadataType, Uri
from zenml.orchestrators import ContainerizedOrchestrator
from zenml.stack import StackValidator

# Custom imports
from orchestrator.my_aws_orchestrator_flavor import (
    StepFunctionsOrchestratorConfig,
    StepFunctionsOrchestratorSettings,
)

if TYPE_CHECKING:
    from zenml.models import PipelineDeploymentResponse
    from zenml.stack import Stack


logger = get_logger(__name__)

ENV_ZENML_STEP_FUNCTIONS_RUN_ID = "ZENML_STEP_FUNCTIONS_RUN_ID"
MAX_POLLING_ATTEMPTS = 100
POLLING_DELAY = 30


def build_dag_levels(deployment: PipelineDeploymentResponse) -> List[List[str]]:
    """Calculate parallel execution levels for a pipeline DAG.

    Args:
        deployment: The pipeline deployment configuration

    Returns:
        List of execution levels where each level contains step names
        that can execute in parallel

    Raises:
        RuntimeError: If a cycle is detected in the dependency graph
    """
    # Build dependency graph and reverse dependency graph
    step_dependencies: Dict[str, Set[str]] = {}
    reverse_dependencies: Dict[str, Set[str]] = {}
    all_steps = set(deployment.step_configurations.keys())

    for step_name, step_config in deployment.step_configurations.items():
        dependencies = set(step_config.spec.upstream_steps)
        step_dependencies[step_name] = dependencies

        for dep in dependencies:
            reverse_dependencies.setdefault(dep, set()).add(step_name)

    # Initialize in-degree map and queue
    in_degree: Dict[str, int] = {
        step: len(deps) for step, deps in step_dependencies.items()
    }

    # Use deque for efficient pops from front
    queue = deque(step for step, count in in_degree.items() if count == 0)
    levels: List[List[str]] = []
    visited: Set[str] = set()

    while queue:
        level_size = len(queue)
        current_level: List[str] = []

        # Process all nodes at current level
        for _ in range(level_size):
            step = queue.popleft()
            if step in visited:
                continue

            current_level.append(step)
            visited.add(step)

            # Update dependencies using reverse graph
            for dependent in reverse_dependencies.get(step, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if current_level:
            levels.append(current_level)

    # Check for cycles
    if len(visited) != len(all_steps):
        unvisited = all_steps - visited
        raise RuntimeError(
            f"Pipeline contains cycles or invalid dependencies. "
            f"Unprocessable steps: {', '.join(unvisited)}"
        )

    return levels


class StepFunctionsOrchestrator(ContainerizedOrchestrator):
    """Orchestrator responsible for running pipelines on AWS Step Functions."""

    @property
    def config(self) -> StepFunctionsOrchestratorConfig:
        """Returns the `StepFunctionsOrchestratorConfig` config.

        Returns:
            The configuration.
        """
        return cast(StepFunctionsOrchestratorConfig, self._config)

    @property
    def validator(self) -> Optional[StackValidator]:
        """Validates the stack.

        In the remote case, checks that the stack contains a container registry,
        image builder and only remote components.

        Returns:
            A `StackValidator` instance.
        """

        def _validate_remote_components(
            stack: "Stack",
        ) -> Tuple[bool, str]:
            for component in stack.components.values():
                if not component.config.is_local:
                    continue

                return False, (
                    f"The Step Functions orchestrator runs pipelines remotely, "
                    f"but the '{component.name}' {component.type.value} is "
                    "a local stack component and will not be available in "
                    "the Step Functions state machine.\nPlease ensure that you always "
                    "use non-local stack components with the Step Functions "
                    "orchestrator."
                )

            return True, ""

        return StackValidator(
            required_components={
                StackComponentType.CONTAINER_REGISTRY,
                StackComponentType.IMAGE_BUILDER,
            },
            custom_validation_function=_validate_remote_components,
        )

    def get_orchestrator_run_id(self) -> str:
        """Returns the run id of the active orchestrator run.

        Important: This needs to be a unique ID and return the same value for
        all steps of a pipeline run.

        Returns:
            The orchestrator run id.

        Raises:
            RuntimeError: If the run id cannot be read from the environment.
        """
        try:
            return os.environ[ENV_ZENML_STEP_FUNCTIONS_RUN_ID]
        except KeyError:
            raise RuntimeError(
                "Unable to read run id from environment variable "
                f"{ENV_ZENML_STEP_FUNCTIONS_RUN_ID}."
            )

    @property
    def settings_class(self) -> Optional[Type["BaseSettings"]]:
        """Settings class for the Step Functions orchestrator.

        Returns:
            The settings class.
        """
        return StepFunctionsOrchestratorSettings

    def _get_step_functions_client(self) -> boto3.client:
        """Method to create the Step Functions client with proper authentication.

        Returns:
            The Step Functions client.

        Raises:
            RuntimeError: If the connector returns the wrong type for the
                session.
        """
        # Get authenticated session
        # Option 1: Service connector
        boto_session: boto3.Session
        if connector := self.get_connector():
            boto_session = connector.connect()
            if not isinstance(boto_session, boto3.Session):
                raise RuntimeError(
                    f"Expected to receive a `boto3.Session` object from the "
                    f"linked connector, but got type `{type(boto_session)}`."
                )
        # Option 2: Explicit configuration
        # Args that are not provided will be taken from the default AWS config.
        else:
            boto_session = boto3.Session(
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                region_name=self.config.region,
                profile_name=self.config.aws_profile,
            )
            # If a role ARN is provided for authentication, assume the role
            if self.config.aws_auth_role_arn:
                sts = boto_session.client("sts")
                response = sts.assume_role(
                    RoleArn=self.config.aws_auth_role_arn,
                    RoleSessionName="zenml-step-functions-orchestrator",
                )
                credentials = response["Credentials"]
                boto_session = boto3.Session(
                    aws_access_key_id=credentials["AccessKeyId"],
                    aws_secret_access_key=credentials["SecretAccessKey"],
                    aws_session_token=credentials["SessionToken"],
                    region_name=self.config.region,
                )
        return boto_session.client("stepfunctions")

    def prepare_or_run_pipeline(
        self,
        deployment: "PipelineDeploymentResponse",
        stack: "Stack",
        environment: Dict[str, str],
    ) -> Iterator[Dict[str, MetadataType]]:
        """Execute pipeline using AWS Step Functions with proper DAG handling"""
        try:
            # Validate AWS permissions and network configuration
            self._validate_aws_configuration()

            # Setup pipeline metadata
            pipeline_name = deployment.pipeline_configuration.name
            state_machine_name = f"zenml-{pipeline_name}-{uuid.uuid4().hex[:6]}"

            # Create optimized task definitions
            task_definitions = self._create_task_definitions(deployment, pipeline_name)

            # Build state machine from DAG structure
            state_machine_definition = self._build_state_machine_definition(
                deployment, task_definitions, environment
            )

            # Deploy and execute state machine
            execution_arn = self._deploy_and_execute(
                state_machine_name, state_machine_definition, deployment
            )

            # Yield metadata with proper execution context
            yield from self._generate_execution_metadata(execution_arn)

        except ClientError as e:
            logger.error(f"AWS API Error: {e.response['Error']['Message']}")
            raise RuntimeError(
                "Failed to execute pipeline on AWS Step Functions"
            ) from e

    def _validate_aws_configuration(self):
        """Pre-flight checks for AWS configuration"""
        self._validate_iam_roles()
        self._validate_network_configuration()

    def _validate_iam_roles(self):
        """Verify required IAM permissions exist"""
        iam = self._get_iam_client()

        required_permissions = [
            "states:CreateStateMachine",
            "states:StartExecution",
            "ecs:RunTask",
            "ecs:DescribeTasks",
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
        ]

        for role_arn in [self.config.execution_role, self.config.task_role]:
            try:
                policy = iam.get_role_policy(
                    RoleName=role_arn.split("/")[-1],
                    PolicyName="AmazonECSTaskExecutionRolePolicy",
                )
                if not all(
                    perm in policy["PolicyDocument"] for perm in required_permissions
                ):
                    raise RuntimeError(
                        f"IAM role {role_arn} missing required permissions"
                    )
            except iam.exceptions.NoSuchEntityException:
                raise RuntimeError(f"IAM role {role_arn} not found or inaccessible")

    def _validate_network_configuration(self):
        """Validate subnet and security group configuration"""
        ec2 = self._get_ec2_client()

        try:
            subnet_info = ec2.describe_subnets(SubnetIds=self.config.subnet_ids)
            for subnet in subnet_info["Subnets"]:
                if not subnet["MapPublicIpOnLaunch"] and self.config.assign_public_ip:
                    logger.warning(
                        "Public IP assignment requested but subnet %s is private",
                        subnet["SubnetId"],
                    )
        except ClientError as e:
            raise RuntimeError(f"Invalid subnet configuration: {e}")

    def _create_task_definitions(
        self, deployment: "PipelineDeploymentResponse", pipeline_name: str
    ) -> Dict[str, str]:
        """Create or reuse ECS task definitions with hashing"""
        task_defs = {}

        for step_name, step_config in deployment.step_configurations.items():
            config_hash = self._hash_step_configuration(step_config)
            family_name = f"zenml-{step_name}-{config_hash[:8]}"

            try:
                existing_def = self._get_existing_task_definition(family_name)
                if existing_def:
                    task_defs[step_name] = existing_def
                    continue
            except ClientError:
                pass

            # Create new task definition
            task_def_arn = self._register_new_task_definition(
                step_name, step_config, pipeline_name, family_name
            )
            task_defs[step_name] = task_def_arn

        return task_defs

    def _hash_step_configuration(self, step_config) -> str:
        """Generate hash of step configuration for reuse checking"""
        hash_data = {
            "image": self.get_image(step_config),
            "cpu": step_config.config.resource_settings.cpu_count,
            "memory": step_config.config.resource_settings.memory,
            "gpu": step_config.config.resource_settings.gpu_count,
        }
        return hashlib.sha256(json.dumps(hash_data).encode()).hexdigest()

    def _build_state_machine_definition(
        self,
        deployment: "PipelineDeploymentResponse",
        task_definitions: Dict[str, str],
        environment: Dict[str, str],
    ) -> Dict[str, Any]:
        """Construct state machine definition with parallel execution"""
        dag_levels = build_dag_levels(deployment)
        states = {
            "Start": {"Type": "Pass", "Next": "PipelineFlow"},
            "PipelineFlow": {"Type": "Parallel", "Branches": []},
            "Success": {"Type": "Succeed"},
            "Failed": {"Type": "Fail", "Error": "ExecutionFailed"},
        }

        for level_num, level in enumerate(dag_levels):
            branch = {
                "StartAt": f"Level_{level_num}_Start",
                "States": self._build_level_states(
                    level, task_definitions, environment, level_num
                ),
            }
            states["PipelineFlow"]["Branches"].append(branch)

        return {
            "Comment": f"ZenML Pipeline: {deployment.pipeline_configuration.name}",
            "StartAt": "Start",
            "States": states,
        }

    def _build_level_states(
        self,
        level: List[str],
        task_definitions: Dict[str, str],
        environment: Dict[str, str],
        level_num: int,
    ) -> Dict[str, Any]:
        """Create states for a single DAG level"""
        states = {}
        for step_name in level:
            states.update(
                self._create_step_state(
                    step_name, task_definitions[step_name], environment
                )
            )

        states[f"Level_{level_num}_Start"] = {
            "Type": "Parallel",
            "Branches": [{"StartAt": step, "States": states}],
            "Next": f"Level_{level_num + 1}_Start"
            if level_num + 1 < len(level)
            else "Success",
        }

        return states

    def _create_step_state(
        self, step_name: str, task_definition_arn: str, environment: Dict[str, str]
    ) -> Dict[str, Any]:
        """Create state machine definition for a single step"""
        return {
            step_name: {
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "LaunchType": "FARGATE",
                    "Cluster": self.config.ecs_cluster_arn,
                    "TaskDefinition": task_definition_arn,
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "Subnets": self.config.subnet_ids,
                            "SecurityGroups": self.config.security_group_ids,
                            "AssignPublicIp": "ENABLED"
                            if self.config.assign_public_ip
                            else "DISABLED",
                        }
                    },
                    "Overrides": {
                        "ContainerOverrides": [
                            {
                                "Name": "zenml-container",
                                "Environment": [
                                    {"Name": k, "Value": v}
                                    for k, v in environment.items()
                                ],
                            }
                        ]
                    },
                },
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 3,
                        "BackoffRate": 2.0,
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "ResultPath": "$.error",
                        "Next": "Failed",
                    }
                ],
                "End": True,
            }
        }

    def _deploy_and_execute(
        self,
        state_machine_name: str,
        definition: Dict[str, Any],
        deployment: "PipelineDeploymentResponse",
    ) -> str:
        """Deploy state machine and start execution"""
        sfn = self._get_step_functions_client()

        try:
            create_response = sfn.create_state_machine(
                name=state_machine_name,
                definition=json.dumps(definition),
                roleArn=self.config.execution_role,
                type="STANDARD",
                tags=[
                    {
                        "key": "zenml-pipeline",
                        "value": deployment.pipeline_configuration.name,
                    },
                    {"key": "zenml-orchestrator", "value": "step-functions"},
                ],
            )
            state_machine_arn = create_response["stateMachineArn"]
        except sfn.exceptions.StateMachineAlreadyExists:
            state_machine_arn = f"arn:aws:states:{self.config.region}:{self.config.account_id}:stateMachine:{state_machine_name}"

        execution = sfn.start_execution(
            stateMachineArn=state_machine_arn, name=f"exec-{uuid.uuid4().hex[:8]}"
        )

        return execution["executionArn"]

    def _generate_execution_metadata(
        self, execution_arn: str
    ) -> Iterator[Dict[str, MetadataType]]:
        """Generate metadata with proper execution context"""
        metadata = {
            METADATA_ORCHESTRATOR_RUN_ID: execution_arn,
            METADATA_ORCHESTRATOR_URL: self._get_execution_console_url(execution_arn),
            METADATA_ORCHESTRATOR_LOGS_URL: self._get_cloudwatch_logs_url(
                execution_arn
            ),
        }

        yield {k: Uri(v) if isinstance(v, str) else v for k, v in metadata.items()}

    def _get_execution_console_url(self, execution_arn: str) -> str:
        """Generate AWS Console URL for execution visualization"""
        region = execution_arn.split(":")[3]
        return (
            f"https://{region}.console.aws.amazon.com/states/home"
            f"?region={region}#/executions/details/{execution_arn}"
        )

    def _get_cloudwatch_logs_url(self, execution_arn: str) -> str:
        """Generate CloudWatch logs URL for execution monitoring"""
        region = execution_arn.split(":")[3]
        return (
            f"https://{region}.console.aws.amazon.com/cloudwatch/home"
            f"?region={region}#logsV2:log-groups/log-group/$252Faws$252Fstates$252F{execution_arn.split(':')[-1]}"
        )
