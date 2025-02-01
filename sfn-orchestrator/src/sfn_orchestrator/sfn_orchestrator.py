"""Improved AWS Step Functions Orchestrator with Parallel Execution and Safety Checks"""

import hashlib
import json
import os
import random
import string
import time
from collections import deque
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    cast,
)

import boto3
from botocore.exceptions import ClientError
from rich import inspect, print
from zenml.config.base_settings import BaseSettings
from zenml.config.resource_settings import ResourceSettings
from zenml.config.step_configurations import Step
from zenml.constants import (
    METADATA_ORCHESTRATOR_LOGS_URL,
    METADATA_ORCHESTRATOR_RUN_ID,
    METADATA_ORCHESTRATOR_URL,
)
from zenml.entrypoints import StepEntrypointConfiguration
from zenml.enums import StackComponentType
from zenml.logger import get_logger
from zenml.metadata.metadata_types import MetadataType, Uri
from zenml.models import PipelineDeploymentResponse
from zenml.orchestrators import ContainerizedOrchestrator
from zenml.stack import StackValidator

from sfn_orchestrator.batch_job_definitions import (
    get_aws_batch_job_definition_settings_from_deployment,
    register_equivalent_job_definitions_for_pipeline,
)

# Custom imports
from sfn_orchestrator.sfn_orchestrator_flavor import (
    StepFunctionsOrchestratorConfig,
    StepFunctionsOrchestratorSettings,
)

if TYPE_CHECKING:
    from zenml.models import PipelineDeploymentResponse
    from zenml.stack import Stack


logger = get_logger(__name__)

ENV_ZENML_STEP_FUNCTIONS_RUN_ID = "ZENML_STEP_FUNCTIONS_RUN_ID"
MAX_TASK_DEFINITION_VERSIONS = 50
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


def build_dag_levels(deployment) -> List[List[str]]:
    """Constructs a DAG level representation of pipeline steps."""
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


def generate_step_functions_metadata(
    execution_arn: str,
) -> Iterator[Dict[str, MetadataType]]:
    region = execution_arn.split(":")[3]
    metadata = {
        METADATA_ORCHESTRATOR_RUN_ID: execution_arn,
        METADATA_ORCHESTRATOR_URL: (
            f"https://{region}.console.aws.amazon.com/states/home"
            f"?region={region}#/executions/details/{execution_arn}"
        ),
        METADATA_ORCHESTRATOR_LOGS_URL: (
            f"https://{region}.console.aws.amazon.com/cloudwatch/home"
            f"?region={region}#logsV2:log-groups/log-group/$252Faws$252Fbatch$252Fjob"
        ),
    }
    yield {k: Uri(v) for k, v in metadata.items()}


def create_state_machine(
    sfn_client: boto3.client,
    name: str,
    definition: Dict[str, Any],
    role_arn: str,
) -> str:
    """Create an AWS Step Functions state machine.

    Args:
        sfn_client: Boto3 Step Functions client
        name: Name of the state machine
        definition: State machine definition dictionary
        role_arn: ARN of the IAM role for the state machine

    Returns:
        The ARN of the created state machine
    """
    response = sfn_client.create_state_machine(
        name=name,
        definition=json.dumps(definition),
        roleArn=role_arn,
        type="STANDARD",
        tags=[],
    )
    state_machine_arn = response["stateMachineArn"]
    print(f"State Machine ARN: {state_machine_arn}")
    return state_machine_arn


def start_state_machine_execution(
    sfn_client: boto3.client,
    state_machine_arn: str,
    pipeline_name: str,
) -> str:
    """Start execution of an AWS Step Functions state machine.

    Args:
        sfn_client: Boto3 Step Functions client
        state_machine_arn: ARN of the state machine to execute
        pipeline_name: Name of the ZenML pipeline

    Returns:
        The ARN of the execution
    """
    execution_name = f"zenml-{pipeline_name}-{int(time.time())}"
    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
    )
    return response["executionArn"]


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
                    f"The Sagemaker orchestrator runs pipelines remotely, "
                    f"but the '{component.name}' {component.type.value} is "
                    "a local stack component and will not be available in "
                    "the Sagemaker step.\nPlease ensure that you always "
                    "use non-local stack components with the Sagemaker "
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

    def prepare_or_run_pipeline(
        self,
        deployment: "PipelineDeploymentResponse",
        stack: "Stack",
        environment: Dict[str, str],
    ) -> Iterator[Dict[str, MetadataType]]:
        print("ran!")
        STEP_FUNCTIONS_ROLE_ARN = (
            "arn:aws:iam::847068433460:role/zenml-hackathon-step-functions-role"
        )

        # inspect(deployment)
        # inspect(stack)
        # inspect(environment)
        # inspect(self)
        # dag_levels = build_dag_levels(deployment)

        # for step_name, step_config in deployment.step_configurations.items():
        #     resource = step_config.config.resource_settings or ResourceSettings()
        #     image = self.get_image(deployment, step_name)
        #     command = StepEntrypointConfiguration.get_entrypoint_command()
        #     arguments = StepEntrypointConfiguration.get_entrypoint_arguments(
        #         step_name=step_name,
        #         deployment_id=deployment.id,
        #     )
        #     entrypoint = command + arguments
        #     step_settings = cast(
        #         StepFunctionsOrchestratorSettings, self.get_settings(step_config)
        #     )

        #     inspect(resource)
        #     # inspect(entrypoint)
        #     print(entrypoint)
        #     inspect(step_settings)

        step_names_to_job_defs: Dict[str, str] = (
            register_equivalent_job_definitions_for_pipeline(
                batch_client=boto3.client("batch", region_name="us-west-2"),
                execution_role_arn=STEP_FUNCTIONS_ROLE_ARN,
                deployment=deployment,
                environment=environment,
                get_image_fn=self.get_image,
            )
        )

        sfn = boto3.client("stepfunctions", region_name="us-west-2")
        name = "ZenML_Batch_Job_StateMachine_DAG_Script"
        state_machine_definition = _create_state_machine_definition(
            deployment, sfn, name=name
        )
        print(state_machine_definition)

        try:
            # Create and execute state machine using helper functions
            state_machine_arn = create_state_machine(
                sfn_client=sfn,
                name=name,
                definition=state_machine_definition,
                role_arn=STEP_FUNCTIONS_ROLE_ARN,
            )

            execution_arn = start_state_machine_execution(
                sfn_client=sfn,
                state_machine_arn=state_machine_arn,
                pipeline_name=deployment.pipeline_configuration.name,
            )

            # Generate metadata using the standalone function
            yield from generate_step_functions_metadata(execution_arn)

        finally:
            # Clean up state machine after execution starts
            try:
                # sfn.delete_state_machine(stateMachineArn=state_machine_arn)
                ...
            except Exception as e:
                logger.warning(f"Failed to delete state machine: {e}")


def _create_state_machine_definition(
    deployment,
    sfn: boto3.client,
    name: str,
) -> Dict[str, Any]:
    """
    Creates an AWS Step Functions state machine for the ZenML pipeline.

    - Uses a **static job definition and job queue**.
    - Supports **parallel execution** of pipeline steps.
    """

    # 🔹 Static AWS Batch settings
    JOB_DEFINITION_NAME = "zenml-fargate-job-def-from-python"
    JOB_QUEUE_NAME = "zenml-fargate-queue-manual"

    # 🔹 Build DAG levels for parallel execution
    dag_levels = build_dag_levels(deployment)

    # 🔹 Define the Step Functions states
    states = {"Start": {"Type": "Pass", "Next": "Level_0"}}

    # Add each level with parallel branches
    for level_num, level in enumerate(dag_levels):
        states[f"Level_{level_num}"] = {
            "Type": "Parallel",
            "Branches": [
                {
                    "StartAt": step,
                    "States": {
                        step: {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::batch:submitJob.sync",
                            "Parameters": {
                                "JobDefinition": JOB_DEFINITION_NAME,
                                "JobQueue": JOB_QUEUE_NAME,
                                "JobName": step,
                            },
                            "End": True,
                        }
                    },
                }
                for step in level
            ],
            "Next": f"Level_{level_num + 1}"
            if level_num < len(dag_levels) - 1
            else "Success",
        }

    # 🔹 Add Success state
    states.update({"Success": {"Type": "Succeed"}})

    # 🔹 Define the full state machine JSON
    definition = {
        "Comment": f"ZenML Pipeline: {deployment.pipeline_configuration.name}",
        "StartAt": "Start",
        "States": states,
    }

    return definition
