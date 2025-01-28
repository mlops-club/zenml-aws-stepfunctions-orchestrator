"""Improved AWS Step Functions Orchestrator with Parallel Execution and Safety Checks"""

import hashlib
import os
import json
import random
import string
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
from zenml.models import PipelineDeploymentResponse
from zenml.config.step_configurations import Step
from zenml.entrypoints import StepEntrypointConfiguration

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
from zenml.config.resource_settings import ResourceSettings

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
MAX_TASK_DEFINITION_VERSIONS = 5
VALID_FARGATE_CPU = {256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
VALID_FARGATE_MEMORY = {
    512: {1024, 2048, 3072, 4096},
    1024: set(range(2048, 8193, 1024)),
    2048: set(range(4096, 16385, 1024)),
    4096: set(range(8192, 30720, 1024)),
    8192: set(range(16384, 61440, 1024)),
    16384: set(range(32768, 122880, 1024)),
    32768: set(range(65536, 122880, 1024)),
    65536: set(range(131072, 237568, 1024)),
}


def _generate_random_string(length: int) -> str:
    """Generate a random string of specified length."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def build_dag_levels(deployment: PipelineDeploymentResponse) -> List[List[str]]:
    step_dependencies = {
        step_name: set(step_config.spec.upstream_steps)
        for step_name, step_config in deployment.step_configurations.items()
    }

    in_degree = {step: len(deps) for step, deps in step_dependencies.items()}
    queue = deque([step for step, count in in_degree.items() if count == 0])
    levels = []

    while queue:
        level = []
        for _ in range(len(queue)):
            step = queue.popleft()
            level.append(step)

            for dependent in step_dependencies:
                if step in step_dependencies[dependent]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        levels.append(level)

    if sum(len(level) for level in levels) != len(deployment.step_configurations):
        raise RuntimeError("Pipeline contains cycles or invalid dependencies")

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

    def _get_boto_client(self, service: str) -> boto3.client:
        """Method to create the Step Functions client with proper authentication.

        Args:
            service: The name of the AWS service to create a client for.

        Returns:
            The Step Functions client.

        Raises:
            RuntimeError: If the connector returns the wrong type for the
                session.
        """
        # Use service connector
        try:
            boto_session: boto3.Session
            if connector := self.get_connector():
                boto_session = connector.connect()
                if not isinstance(boto_session, boto3.Session):
                    raise RuntimeError(
                        f"Expected to receive a `boto3.Session` object from the "
                        f"linked connector, but got type `{type(boto_session)}`."
                    )
            else:
                raise RuntimeError("Service connector is not set up correctly.")
        except Exception as e:
            logger.error(f"Error connecting to AWS using service connector: {e}.")
            # Use local client
            boto_session = boto3.Session(
                region_name=self.config.region,
            )

        return boto_session.client(service)

    def prepare_or_run_pipeline(
        self,
        deployment: "PipelineDeploymentResponse",
        stack: "Stack",
        environment: Dict[str, str],
    ) -> Iterator[Dict[str, MetadataType]]:
        sfn = self._get_boto_client("stepfunctions")
        ecs = self._get_boto_client("ecs")
        state_machine_arn = None

        try:
            pipeline_name = deployment.pipeline_configuration.name
            state_machine_name = f"zenml-{pipeline_name}-{_generate_random_string(6)}"
            execution_id = f"zenml-{_generate_random_string(8)}"
            environment[ENV_ZENML_STEP_FUNCTIONS_RUN_ID] = execution_id

            task_definitions = self._create_task_definitions(
                deployment, environment, ecs
            )
            state_machine_arn = self._create_state_machine_definition(
                deployment, sfn, task_definitions, state_machine_name
            )

            response = sfn.start_execution(
                stateMachineArn=state_machine_arn,
                name=execution_id,
            )
            execution_arn = response["executionArn"]
            yield from self._generate_execution_metadata(
                execution_arn, task_definitions
            )

        except ClientError as e:
            logger.error(f"AWS API Error: {e.response['Error']['Message']}")
            raise RuntimeError("Pipeline execution failed") from e
        finally:
            if state_machine_arn:
                self._cleanup_resources(sfn, ecs, state_machine_arn, task_definitions)

    def _create_task_definitions(
        self,
        deployment: "PipelineDeploymentResponse",
        environment: Dict[str, str],
        ecs: boto3.client,
    ) -> Dict[str, str]:
        task_defs = {}
        for step_name, step_config in deployment.step_configurations.items():
            resource = step_config.config.resource_settings or ResourceSettings()
            image = self.get_image(deployment, step_name)
            command = StepEntrypointConfiguration.get_entrypoint_command()
            arguments = StepEntrypointConfiguration.get_entrypoint_arguments(
                step_name=step_name,
                deployment_id=deployment.id,
            )
            entrypoint = command + arguments
            step_settings = cast(
                StepFunctionsOrchestratorSettings, self.get_settings(step_config)
            )
            config_hash = hashlib.sha256(
                json.dumps(
                    {
                        "image": image,
                        "env": sorted(environment.items()),
                        "command": entrypoint,
                        "cpu": resource.cpu_count,
                        "memory": resource.memory,
                        "gpu": resource.gpu_count,
                    },
                    sort_keys=True,
                ).encode()
            ).hexdigest()
            family_name = f"zenml-{step_name}-{config_hash[:8]}"

            self._cleanup_old_task_definitions(ecs, family_name)

            try:
                task_def_arn = self._get_existing_task_definition(ecs, family_name)
            except ClientError:
                self._validate_fargate_resources(resource)

                response = ecs.register_task_definition(
                    family=family_name,
                    networkMode=step_settings.network_mode,
                    requiresCompatibilities=step_settings.requires_compatibilities,
                    cpu=str(resource.cpu_count),
                    memory=str(resource.memory),
                    executionRoleArn=self.config.execution_role,
                    taskRoleArn=self.config.task_role,
                    containerDefinitions=[
                        {
                            "name": "zenml-container",
                            "image": image,
                            "command": entrypoint,
                            "environment": [
                                {"name": k, "value": v} for k, v in environment.items()
                            ],
                            "logConfiguration": {
                                "logDriver": "awslogs",
                                "options": {
                                    "awslogs-group": f"/ecs/{family_name}",
                                    "awslogs-region": self.config.region,
                                    "awslogs-stream-prefix": "zenml",
                                },
                            },
                        }
                    ],
                )
                task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
                task_defs[step_name] = task_def_arn

        return task_defs

    def _cleanup_old_task_definitions(self, ecs: boto3.client, family_name: str):
        try:
            versions = ecs.list_task_definitions(
                family_prefix=family_name, sort="DESC"
            )["taskDefinitionArns"]
            for arn in versions[MAX_TASK_DEFINITION_VERSIONS:]:
                ecs.deregister_task_definition(taskDefinition=arn)
        except ClientError as e:
            logger.warning(f"Failed to clean up task definitions: {e}")

    def _validate_fargate_resources(self, settings: ResourceSettings):
        if settings.cpu_count not in VALID_FARGATE_CPU:
            raise ValueError(
                f"Invalid CPU value {settings.cpu_count}. Valid values: {VALID_FARGATE_CPU}"
            )
        if settings.memory not in VALID_FARGATE_MEMORY.get(settings.cpu_count, set()):
            raise ValueError(
                f"Memory {settings.memory}MB invalid for {settings.cpu_count} CPU units"
            )

    def _create_state_machine_definition(
        self,
        deployment: "PipelineDeploymentResponse",
        sfn: boto3.client,
        task_definitions: Dict[str, str],
        name: str,
    ) -> Dict[str, Any]:
        dag_levels = build_dag_levels(deployment)
        states = {"Start": {"Type": "Pass", "Next": "Level_0"}}

        for level_num, level in enumerate(dag_levels):
            states[f"Level_{level_num}"] = {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": step,
                        "States": {
                            step: self._create_step_state(task_definitions[step])
                        },
                    }
                    for step in level
                ],
                "Next": f"Level_{level_num + 1}"
                if level_num < len(dag_levels) - 1
                else "Success",
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "ResultPath": "$.error",
                        "Next": "Failed",
                    }
                ],
            }

        states.update(
            {
                "Success": {"Type": "Succeed"},
                "Failed": {"Type": "Fail", "Error": "ExecutionFailed"},
            }
        )

        definition = {
            "Comment": f"ZenML Pipeline: {deployment.pipeline_configuration.name}",
            "StartAt": "Start",
            "States": states,
        }
        response = sfn.create_state_machine(
            name=name,
            definition=json.dumps(definition),
            roleArn=self.config.execution_role,
            type="STANDARD",
            tags=[{"key": "zenml-pipeline", "value": name}],
        )
        return response["stateMachineArn"]

    def _create_step_state(self, task_definition_arn: str) -> Dict[str, Any]:
        return {
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
            },
            "Retry": [
                {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 30,
                    "MaxAttempts": 3,
                    "BackoffRate": 2.0,
                }
            ],
            "End": True,
        }

    def _generate_execution_metadata(
        self,
        execution_arn: str,
        task_definitions: Dict[str, str],
    ) -> Iterator[Dict[str, MetadataType]]:
        region = execution_arn.split(":")[3]
        first_task_family = (
            list(task_definitions.values())[0].split("/")[-1].split(":")[0]
        )

        metadata = {
            METADATA_ORCHESTRATOR_RUN_ID: execution_arn,
            METADATA_ORCHESTRATOR_URL: (
                f"https://{region}.console.aws.amazon.com/states/home"
                f"?region={region}#/executions/details/{execution_arn}"
            ),
            METADATA_ORCHESTRATOR_LOGS_URL: (
                f"https://{region}.console.aws.amazon.com/cloudwatch/home"
                f"?region={region}#logsV2:log-groups/log-group/$252Fecs$252F{first_task_family}"
            ),
        }

        yield {k: Uri(v) for k, v in metadata.items()}

    def _cleanup_resources(
        self,
        sfn: boto3.client,
        ecs: boto3.client,
        state_machine_arn: str,
        task_definitions: Dict[str, str],
    ):
        try:
            sfn.delete_state_machine(stateMachineArn=state_machine_arn)
        except ClientError as e:
            logger.warning(f"Failed to delete state machine: {e}")

        for arn in task_definitions.values():
            try:
                family = arn.split("/")[-1].split(":")[0]
                self._cleanup_old_task_definitions(ecs, family)
            except ClientError as e:
                logger.warning(f"Failed to clean up task definitions: {e}")

    def _get_existing_task_definition(self, ecs: boto3.client, family_name: str) -> str:
        """Retrieve an existing task definition ARN for a given family name.

        Args:
            ecs: The ECS client.
            family_name: The family name of the task definition.

        Returns:
            The ARN of the existing task definition.

        Raises:
            ClientError: If no task definition is found.
        """
        try:
            response = ecs.list_task_definitions(
                familyPrefix=family_name, status="ACTIVE", sort="DESC", maxResults=1
            )
            task_definition_arns = response.get("taskDefinitionArns", [])
            if not task_definition_arns:
                raise ClientError(
                    {
                        "Error": {
                            "Code": "NoSuchTaskDefinition",
                            "Message": f"No active task definition found for family {family_name}",
                        }
                    },
                    "ListTaskDefinitions",
                )
            return task_definition_arns[0]
        except ClientError as e:
            logger.error(
                f"Failed to retrieve task definition for family {family_name}: {e}"
            )
            raise
