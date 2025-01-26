"""Implementation of the AWS Step Functions orchestrator."""

import os
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    Optional,
    Tuple,
    Type,
    cast,
)
from uuid import UUID

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
        return boto_session.client('stepfunctions')

    def prepare_or_run_pipeline(
        self,
        deployment: "PipelineDeploymentResponse",
        stack: "Stack",
        environment: Dict[str, str],
    ) -> Iterator[Dict[str, MetadataType]]:
        """Prepares or runs a pipeline on AWS Step Functions.

        Args:
            deployment: The deployment to prepare or run.
            stack: The stack to run on.
            environment: Environment variables to set in the orchestration
                environment.

        Raises:
            RuntimeError: If a connector is used that does not return a
                `boto3.Session` object.
            TypeError: If the network_config passed is not compatible.

        Yields:
            A dictionary of metadata related to the pipeline run.
        """
        # TODO: Implement Step Functions specific pipeline execution logic
        # This will involve:
        # 1. Creating a state machine definition from the pipeline steps
        # 2. Creating/updating the state machine
        # 3. Starting an execution
        # 4. Returning execution metadata
        
        raise NotImplementedError(
            "Step Functions pipeline execution not yet implemented"
        )

    def get_pipeline_run_metadata(
        self, run_id: UUID
    ) -> Dict[str, "MetadataType"]:
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
                "The stack that the run was executed on is not available "
                "anymore."
            )

        # Make sure that the run belongs to this orchestrator
        assert (
            self.id
            == run.stack.components[StackComponentType.ORCHESTRATOR][0].id
        )

        # Initialize the Step Functions client
        sfn_client = self._get_step_functions_client()

        # Fetch the status of the State Machine execution
        if METADATA_ORCHESTRATOR_RUN_ID in run.run_metadata:
            run_id = run.run_metadata[METADATA_ORCHESTRATOR_RUN_ID]
        elif run.orchestrator_run_id is not None:
            run_id = run.orchestrator_run_id
        else:
            raise ValueError(
                "Can not find the orchestrator run ID, thus can not fetch "
                "the status."
            )
        
        response = sfn_client.describe_execution(
            executionArn=run_id
        )
        status = response["status"]

        # Map Step Functions status to ZenML ExecutionStatus
        if status in ["RUNNING"]:
            return ExecutionStatus.RUNNING
        elif status in ["FAILED", "TIMED_OUT", "ABORTED"]:
            return ExecutionStatus.FAILED
        elif status in ["SUCCEEDED"]:
            return ExecutionStatus.COMPLETED
        else:
            raise ValueError(f"Unknown status for the state machine execution: {status}")

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
        # Metadata
        metadata: Dict[str, MetadataType] = {}

        # Orchestrator Run ID
        if run_id := self._compute_orchestrator_run_id(execution):
            metadata[METADATA_ORCHESTRATOR_RUN_ID] = run_id

        # URL to the Step Functions console view
        if orchestrator_url := self._compute_orchestrator_url(execution):
            metadata[METADATA_ORCHESTRATOR_URL] = Uri(orchestrator_url)

        # URL to the corresponding CloudWatch logs
        if logs_url := self._compute_orchestrator_logs_url(
            execution, settings
        ):
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
            region_name, state_machine_name, execution_id = (
                dissect_state_machine_execution_arn(execution.execution_arn)
            )

            return (
                f"https://{region_name}.console.aws.amazon.com/states/home"
                f"?region={region_name}#/executions/details/{execution.execution_arn}"
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
                execution.execution_arn
            )

            return (
                f"https://{region_name}.console.aws.amazon.com/"
                f"cloudwatch/home?region={region_name}#logsV2:log-groups/log-group"
                f"/$252Faws$252Fstates$252F{execution_id}"
            )
        except Exception as e:
            logger.warning(
                f"There was an issue while extracting the logs url: {e}"
            )
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
            return str(execution.execution_arn)

        except Exception as e:
            logger.warning(
                f"There was an issue while extracting the execution run ID: {e}"
            )
            return None
