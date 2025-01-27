"""Implementation of the AWS Step Functions orchestrator."""

import os
import re
import json
import uuid
import time
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
from uuid import UUID
from collections import OrderedDict

import boto3

from zenml.config.base_settings import BaseSettings
from zenml.constants import (
    METADATA_ORCHESTRATOR_LOGS_URL,
    METADATA_ORCHESTRATOR_RUN_ID,
    METADATA_ORCHESTRATOR_URL,
)
from zenml.enums import ExecutionStatus, StackComponentType
from orchestrator.my_aws_orchestrator_flavor import (
    StepFunctionsOrchestratorConfig,
    StepFunctionsOrchestratorSettings,
)
from zenml.logger import get_logger
from zenml.metadata.metadata_types import MetadataType, Uri
from zenml.orchestrators import ContainerizedOrchestrator
from zenml.stack import StackValidator

if TYPE_CHECKING:
    from zenml.models import PipelineDeploymentResponse, PipelineRunResponse
    from zenml.stack import Stack

ENV_ZENML_STEP_FUNCTIONS_RUN_ID = "ZENML_STEP_FUNCTIONS_RUN_ID"
MAX_POLLING_ATTEMPTS = 100
POLLING_DELAY = 30

logger = get_logger(__name__)

if TYPE_CHECKING:
    from zenml.config.resource_settings import ResourceSettings


def get_orchestrator_run_name(pipeline_name: str) -> str:
    """Generate a unique name for the orchestrator run.

    Args:
        pipeline_name: Name of the pipeline.

    Returns:
        A unique run name.
    """
    return f"zenml-{pipeline_name}-{uuid.uuid4().hex[:8]}"


def dissect_state_machine_execution_arn(
    state_machine_execution_arn: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract region name, state machine name, and execution id from the ARN.

    Args:
        state_machine_execution_arn: the state machine execution ARN

    Returns:
        Region Name, State Machine Name, Execution ID in order
    """
    # Extract region_name
    region_match = re.search(r"states:(.*?):", state_machine_execution_arn)
    region_name = region_match.group(1) if region_match else None

    # Extract state_machine_name
    state_machine_match = re.search(
        r"stateMachine/(.*?)/execution", state_machine_execution_arn
    )
    state_machine_name = state_machine_match.group(1) if state_machine_match else None

    # Extract execution_id
    execution_match = re.search(r"execution/(.*)", state_machine_execution_arn)
    execution_id = execution_match.group(1) if execution_match else None

    return region_name, state_machine_name, execution_id


def build_dag_levels(deployment: "PipelineDeploymentResponse") -> List[List[str]]:
    """Returns a list of lists, representing 'levels' of a DAG:
    Each sub-list contains the step names that can safely run in parallel.
    """
    # Build adjacency list: step -> upstream steps
    step_to_upstreams = {}
    all_steps = list(deployment.step_configurations.keys())
    for step_name, step_config in deployment.step_configurations.items():
        step_to_upstreams[step_name] = step_config.spec.upstream_steps or []

    # Track in-degrees (how many prerequisites each step has)
    in_degree = {s: 0 for s in all_steps}
    for s, upstreams in step_to_upstreams.items():
        for us in upstreams:
            in_degree[s] = in_degree[s] + 1

    # Initialize queue with root steps (in-degree = 0)
    from collections import deque

    queue = deque([s for s in all_steps if in_degree[s] == 0])
    levels = []
    visited = set()

    while queue:
        # All steps in the queue can run in parallel
        current_level = list(queue)
        levels.append(current_level)

        # Prepare for next level
        next_queue = []
        for step in current_level:
            visited.add(step)
        while current_level:
            step = current_level.pop()
            # Decrease in-degree of steps that depend on "step"
            for candidate in all_steps:
                if step in step_to_upstreams[candidate]:
                    in_degree[candidate] -= 1
                    if in_degree[candidate] == 0:
                        next_queue.append(candidate)

        queue = deque(next_queue)

    # If visited < all_steps, there's a cycle or some error, handle accordingly
    if len(visited) < len(all_steps):
        raise RuntimeError("Cycle detected in pipeline steps or missing references.")

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

    def _create_or_update_task_definition(
        self,
        step_name: str,
        image: str,
        resource_settings: Optional["ResourceSettings"],
        step_settings: "StepFunctionsOrchestratorSettings",
        pipeline_name: str,
    ) -> str:
        """Dynamically create or update an ECS task definition.

        Args:
            step_name: Name of the step
            image: Docker image to use
            resource_settings: CPU/Memory requirements
            step_settings: Step-specific settings
            pipeline_name: Name of the pipeline

        Returns:
            Task definition ARN
        """
        # Initialize ECS client
        ecs_client = self._get_ecs_client()

        # Calculate CPU and memory
        cpu = (
            str(int(resource_settings.cpu_count * 1024))
            if resource_settings and resource_settings.cpu_count
            else "256"
        )
        memory = (
            str(int(resource_settings.memory * 1024))
            if resource_settings and resource_settings.memory
            else "512"
        )

        # Create task definition
        task_definition = {
            "family": f"zenml-{step_name}",
            "networkMode": "awsvpc",
            "requiresCompatibilities": ["FARGATE"],
            "cpu": cpu,
            "memory": memory,
            "executionRoleArn": self.config.execution_role,
            "taskRoleArn": self.config.task_role,
            "containerDefinitions": [
                {
                    "name": step_settings.container_name,
                    "image": image,
                    "essential": True,
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": f"/ecs/zenml-{step_name}",
                            "awslogs-region": self.config.region,
                            "awslogs-stream-prefix": "ecs",
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            # Add tags to task definition
            "tags": [
                {"key": "zenml.pipeline_name", "value": pipeline_name},
                {"key": "zenml.step_name", "value": step_name},
                {"key": "zenml.orchestrator", "value": "step_functions"},
                {"key": "zenml.resource_type", "value": "task_definition"},
                *[{"key": k, "value": v} for k, v in step_settings.tags.items()],
            ],
        }

        try:
            response = ecs_client.register_task_definition(**task_definition)
            return response["taskDefinition"]["taskDefinitionArn"]
        except Exception as e:
            raise RuntimeError(f"Failed to register task definition: {str(e)}")

    def _create_step_state_machine(
        self,
        step_name: str,
        task_definition_arn: str,
        environment: Dict[str, str],
        step_settings: "StepFunctionsOrchestratorSettings",
        next_step: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Creates states for a single step execution (run + choice + retry logic).
        
        Args:
            step_name: Name of the step
            task_definition_arn: ARN of ECS task definition
            environment: Environment variables
            step_settings: Step-specific settings
            next_step: Name of the next step (or None if final step)
            
        Returns:
            Dictionary of states for this step
        """
        # Generate state names for this step
        run_state = f"{step_name}_run"
        choice_state = f"{step_name}_choice"
        retry_state = f"{step_name}_retry"
        
        states = {}
        
        # Main execution state
        states[run_state] = {
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
                        "AssignPublicIp": "ENABLED" if step_settings.assign_public_ip else "DISABLED",
                    }
                },
                "Overrides": {
                    "ContainerOverrides": [{
                        "Name": step_settings.container_name,
                        "Environment": [
                            {"Name": key, "Value": value}
                            for key, value in environment.items()
                        ]
                    }]
                },
                "Tags": [
                    {"key": "zenml.step_name", "value": step_name},
                    {"key": "zenml.resource_type", "value": "ecs_task"},
                    *[{"key": k, "value": v} for k, v in step_settings.tags.items()]
                ]
            },
            "ResultPath": "$.taskResult",
            "Next": choice_state,
            "Retry": [
                {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": step_settings.max_retries,
                    "BackoffRate": 2.0
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Failed"
                }
            ]
        }
        
        # Choice state to handle retry logic
        states[choice_state] = {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.taskResult.ExitCode",
                    "NumericEquals": 0,
                    "Next": next_step if next_step else "Success"
                },
                {
                    "Variable": "$.taskResult.ExitCode",
                    "NumericExists": True,
                    "Next": retry_state
                }
            ],
            "Default": retry_state
        }
        
        # Retry state
        states[retry_state] = {
            "Type": "Wait",
            "Seconds": 30,
            "Next": run_state
        }
        
        return states

    def prepare_or_run_pipeline(
        self,
        deployment: "PipelineDeploymentResponse",
        stack: "Stack",
        environment: Dict[str, str],
    ) -> Iterator[Dict[str, MetadataType]]:
        """Prepares or runs a pipeline on AWS Step Functions.
        
        Args:
            deployment: The deployment to prepare or run
            stack: The stack to run on
            environment: Environment variables
            
        Yields:
            Pipeline metadata
        """
        # 1. Basic setup
        pipeline_name = deployment.pipeline_configuration.name
        state_machine_name = f"zenml-{pipeline_name}-{uuid.uuid4().hex[:8]}"

        # 2. Create task definitions for all steps
        step_task_defs = {}
        for step_name, step_config in deployment.step_configurations.items():
            task_def_arn = self._create_or_update_task_definition(
                step_name=step_name,
                image=self.get_image(deployment, step_name),
                resource_settings=step_config.config.resource_settings,
                step_settings=cast(StepFunctionsOrchestratorSettings, self.get_settings(step_config)),
                pipeline_name=pipeline_name
            )
            step_task_defs[step_name] = task_def_arn

        # 3. Build linear sequence of steps
        step_sequence = self._build_step_sequence(deployment)
        
        # 4. Create state machine definition
        states = {
            "Success": {"Type": "Succeed"},
            "Failed": {
                "Type": "Fail",
                "Error": "StepFailed",
                "Cause": "Step execution failed"
            }
        }
        
        # Add states for each step
        for i, step_name in enumerate(step_sequence):
            next_step = (
                f"{step_sequence[i+1]}_run"
                if i < len(step_sequence) - 1
                else None
            )
            
            step_states = self._create_step_state_machine(
                step_name=step_name,
                task_definition_arn=step_task_defs[step_name],
                environment=environment,
                step_settings=cast(
                    StepFunctionsOrchestratorSettings,
                    self.get_settings(deployment.step_configurations[step_name])
                ),
                next_step=next_step
            )
            states.update(step_states)

        # 5. Create final state machine definition
        state_machine_definition = {
            "Comment": f"ZenML pipeline: {pipeline_name}",
            "StartAt": f"{step_sequence[0]}_run",
            "States": states
        }

        # 6. Create/update and start state machine
        sfn_client = self._get_step_functions_client()
        try:
            response = sfn_client.create_state_machine(
                name=state_machine_name,
                definition=json.dumps(state_machine_definition),
                roleArn=self.config.execution_role,
                type=cast(
                    StepFunctionsOrchestratorSettings,
                    self.get_settings(deployment)
                ).state_machine_type,
                tags=[
                    {"key": "zenml.pipeline_name", "value": pipeline_name},
                    {"key": "zenml.orchestrator", "value": "step_functions"}
                ]
            )
            state_machine_arn = response["stateMachineArn"]
        except sfn_client.exceptions.StateMachineAlreadyExists:
            state_machine_arn = (
                f"arn:aws:states:{self.config.region}:"
                f"{self.config.account_id}:stateMachine:{state_machine_name}"
            )
            sfn_client.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=json.dumps(state_machine_definition),
                roleArn=self.config.execution_role
            )

        # 7. Start execution
        execution = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"{state_machine_name}-{uuid.uuid4()}"
        )

        # 8. Return metadata
        yield from self.compute_metadata(
            execution=execution,
            settings=cast(
                StepFunctionsOrchestratorSettings,
                self.get_settings(deployment)
            )
        )

    def _build_step_sequence(
        self,
        deployment: "PipelineDeploymentResponse"
    ) -> List[str]:
        """Builds a linear sequence of steps that respects dependencies.
        
        Args:
            deployment: The pipeline deployment
            
        Returns:
            List of step names in execution order
        """
        # Build dependency graph
        graph = {
            name: config.spec.upstream_steps
            for name, config in deployment.step_configurations.items()
        }
        
        # Simple topological sort
        sequence = []
        visited = set()
        
        def visit(step: str):
            if step in visited:
                return
            for upstream in graph[step]:
                visit(upstream)
            visited.add(step)
            sequence.append(step)
            
        for step in graph:
            visit(step)
            
        return sequence

    def get_pipeline_run_metadata(self, run_id: UUID) -> Dict[str, "MetadataType"]:
        """Get general component-specific metadata for a pipeline run.

        Args:
            run_id: The ID of the pipeline run.

        Returns:
            A dictionary of metadata.
        """
        execution_arn = os.environ[ENV_ZENML_STEP_FUNCTIONS_RUN_ID]
        run_metadata: Dict[str, "MetadataType"] = {
            "execution_arn": execution_arn,
        }

        return run_metadata

    def fetch_status(self, run: "PipelineRunResponse") -> ExecutionStatus:
        """Refreshes the status of a specific pipeline run.

        Args:
            run: The run that was executed by this orchestrator.

        Returns:
            the actual status of the pipeline job.

        Raises:
            AssertionError: If the run was not executed by to this orchestrator.
            ValueError: If it fetches an unknown state or if we can not fetch
                the orchestrator run ID.
        """
        # Make sure that the stack exists and is accessible
        if run.stack is None:
            raise ValueError(
                "The stack that the run was executed on is not available anymore."
            )

        # Make sure that the run belongs to this orchestrator
        assert self.id == run.stack.components[StackComponentType.ORCHESTRATOR][0].id

        # Initialize the Step Functions client
        sfn_client = self._get_step_functions_client()

        # Fetch the status of the State Machine execution
        if METADATA_ORCHESTRATOR_RUN_ID in run.run_metadata:
            run_id = run.run_metadata[METADATA_ORCHESTRATOR_RUN_ID]
        elif run.orchestrator_run_id is not None:
            run_id = run.orchestrator_run_id
        else:
            raise ValueError(
                "Can not find the orchestrator run ID, thus can not fetch the status."
            )

        response = sfn_client.describe_execution(executionArn=run_id)
        status = response["status"]

        # Map Step Functions status to ZenML ExecutionStatus
        if status in ["RUNNING"]:
            return ExecutionStatus.RUNNING
        elif status in ["FAILED", "TIMED_OUT", "ABORTED"]:
            return ExecutionStatus.FAILED
        elif status in ["SUCCEEDED"]:
            return ExecutionStatus.COMPLETED
        else:
            raise ValueError(
                f"Unknown status for the state machine execution: {status}"
            )

    def compute_metadata(
        self,
        execution: Any,
        settings: StepFunctionsOrchestratorSettings,
    ) -> Iterator[Dict[str, MetadataType]]:
        """Generate run metadata based on the Step Functions Execution.

        Args:
            execution: The corresponding Step Functions execution object.
            settings: The Step Functions orchestrator settings.

        Yields:
            A dictionary of metadata related to the pipeline run.
        """
        metadata: Dict[str, MetadataType] = {}

        # Orchestrator Run ID
        if run_id := self._compute_orchestrator_run_id(execution):
            metadata[METADATA_ORCHESTRATOR_RUN_ID] = run_id

        # URL to the Step Functions console view
        if orchestrator_url := self._compute_orchestrator_url(execution):
            metadata[METADATA_ORCHESTRATOR_URL] = Uri(orchestrator_url)

        # URL to the corresponding CloudWatch logs
        if logs_url := self._compute_orchestrator_logs_url(execution, settings):
            metadata[METADATA_ORCHESTRATOR_LOGS_URL] = Uri(logs_url)

        yield metadata

    @staticmethod
    def _compute_orchestrator_url(
        execution: Any,
    ) -> Optional[str]:
        """Generate the AWS Step Functions Console URL for the execution.

        Args:
            execution: The corresponding Step Functions execution object.

        Returns:
             the URL to the execution view in AWS Step Functions console.
        """
        try:
            region_name, _, _ = dissect_state_machine_execution_arn(
                execution["executionArn"]
            )
            return (
                f"https://{region_name}.console.aws.amazon.com/states/home"
                f"?region={region_name}#/executions/details/{execution['executionArn']}"
            )
        except Exception as e:
            logger.warning(
                f"There was an issue while extracting the execution url: {e}"
            )
            return None

    @staticmethod
    def _compute_orchestrator_logs_url(
        execution: Any,
        settings: StepFunctionsOrchestratorSettings,
    ) -> Optional[str]:
        """Generate the CloudWatch URL for execution logs.

        Args:
            execution: The corresponding Step Functions execution object.
            settings: The Step Functions orchestrator settings.

        Returns:
            the URL querying the execution logs in CloudWatch.
        """
        try:
            region_name, _, execution_id = dissect_state_machine_execution_arn(
                execution["executionArn"]
            )
            return (
                f"https://{region_name}.console.aws.amazon.com/"
                f"cloudwatch/home?region={region_name}#logsV2:log-groups/log-group"
                f"/$252Faws$252Fstates$252F{execution_id}"
            )
        except Exception as e:
            logger.warning(f"There was an issue while extracting the logs url: {e}")
            return None

    @staticmethod
    def _compute_orchestrator_run_id(
        execution: Any,
    ) -> Optional[str]:
        """Fetch the Orchestrator Run ID from the execution.

        Args:
            execution: The corresponding Step Functions execution object.

        Returns:
             the Execution ARN of the run in Step Functions.
        """
        try:
            return str(execution["executionArn"])
        except Exception as e:
            logger.warning(
                f"There was an issue while extracting the execution run ID: {e}"
            )
            return None

    def _configure_task_resources(
        self,
        step_definition: Dict[str, Any],
        resource_settings: Optional["ResourceSettings"],
    ) -> None:
        """Configure CPU and memory for an ECS task.

        Args:
            step_definition: The Step Functions task definition.
            resource_settings: Resource settings for the step.
        """
        if not resource_settings:
            return

        container_overrides = step_definition["Parameters"]["Overrides"]

        # Configure CPU and memory at task level
        if resource_settings.cpu_count:
            container_overrides["CPU"] = str(int(resource_settings.cpu_count * 1024))

        if resource_settings.memory:
            container_overrides["Memory"] = str(int(resource_settings.memory * 1024))

        if resource_settings.gpu_count:
            logger.warning(
                "GPU configuration is not supported in ECS Fargate. "
                "To use GPUs, consider using AWS Batch or SageMaker instead."
            )
