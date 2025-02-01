"""Improved AWS Step Functions Orchestrator with Parallel Execution and Safety Checks"""

import hashlib
import json
import os
import random
import string
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
from rich import inspect, print
import boto3
from botocore.exceptions import ClientError
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
        STEP_FUNCTIONS_ROLE_ARN = "arn:aws:iam::847068433460:role/zenml-hackathon-step-functions-role"

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
        sfn = boto3.client("stepfunctions", region_name="us-west-2")
        name="ZenML_Batch_Job_StateMachine_DAG_Script"
        state_machine_definition = self._create_state_machine_definition(deployment, sfn, name=name)
        print(state_machine_definition)
        
        response = sfn.create_state_machine(
            name=name,
            definition=json.dumps(state_machine_definition),
            roleArn=STEP_FUNCTIONS_ROLE_ARN,
            type="STANDARD",  # Can be changed to "EXPRESS" if needed
            tags=[],
        )
        
        print(f"State Machine ARN: {response['stateMachineArn']}")
        
        


    def _create_state_machine_definition(
        self,
        deployment,
        sfn: boto3.client,
        name: str,
    ) -> Dict[str, Any]:
        """
        Creates an AWS Step Functions state machine for the ZenML pipeline.
        
        - Uses a **static job definition and job queue**.
        - Supports **parallel execution** of pipeline steps.
        - Adds **error handling** and **retries**.
        """

        # 🔹 Static AWS Batch settings
        JOB_DEFINITION_NAME = "zenml-fargate-job-def-from-python"
        JOB_QUEUE_NAME = "zenml-fargate-queue-manual"

        # 🔹 Build DAG levels for parallel execution
        dag_levels = build_dag_levels(deployment)

        # 🔹 Define the Step Functions states
        states = {"Start": {"Type": "Pass", "Next": "Level_0"}}

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
                                    "JobName": step
                                },
                                "Retry": [
                                    {
                                        "ErrorEquals": ["States.TaskFailed"],
                                        "IntervalSeconds": 10,
                                        "MaxAttempts": 3,
                                        "BackoffRate": 2.0
                                    }
                                ],
                                "Catch": [
                                    {
                                        "ErrorEquals": ["States.ALL"],
                                        "ResultPath": "$.error",
                                        "Next": "Failed"
                                    }
                                ],
                                "End": True
                            }
                        }
                    }
                    for step in level
                ],
                "Next": f"Level_{level_num + 1}" if level_num < len(dag_levels) - 1 else "Success",
            }

        # 🔹 Add Success & Failure states
        states.update(
            {
                "Success": {"Type": "Succeed"},
                "Failed": {"Type": "Fail", "Error": "ExecutionFailed"}
            }
        )

        # 🔹 Define the full state machine JSON
        definition = {
            "Comment": f"ZenML Pipeline: {deployment.pipeline_configuration.name}",
            "StartAt": "Start",
            "States": states,
        }

        # # 🔹 Create the State Machine in AWS Step Functions
        # response = sfn.create_state_machine(
        #     name=name,
        #     definition=json.dumps(definition),
        #     roleArn=self.config.execution_role,
        #     type="STANDARD",  # Can be changed to "EXPRESS" if needed
        #     tags=[],
        # )

        # return response["stateMachineArn"]
        return definition