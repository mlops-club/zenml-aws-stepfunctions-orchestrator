"""AWS Step Functions orchestrator flavor."""

from typing import TYPE_CHECKING, Dict, List, Optional, Type

from zenml.config.base_settings import BaseSettings
from zenml.integrations.aws import AWS_RESOURCE_TYPE
from zenml.models import ServiceConnectorRequirements
from zenml.orchestrators import BaseOrchestratorConfig
from zenml.orchestrators.base_orchestrator import BaseOrchestratorFlavor
from zenml.utils.secret_utils import SecretField

if TYPE_CHECKING:
    from orchestrator.my_aws_orchestrator import StepFunctionsOrchestrator

DEFAULT_STATE_MACHINE_TYPE = "STANDARD"
AWS_STEP_FUNCTIONS_ORCHESTRATOR_FLAVOR = "aws_step_functions"


class StepFunctionsOrchestratorSettings(BaseSettings):
    """Settings for the AWS Step Functions orchestrator.

    Attributes:
        synchronous: If `True`, the client running a pipeline using this
            orchestrator waits until all steps finish running. If `False`,
            the client returns immediately and the pipeline is executed
            asynchronously. Defaults to `True`.
        state_machine_type: The type of state machine to create. Can be either
            'STANDARD' or 'EXPRESS'. Defaults to 'STANDARD'.
        tags: Tags to apply to the state machine and ECS tasks.
        max_runtime_in_seconds: The maximum runtime in seconds for the
            state machine execution. Defaults to 24 hours.
        container_name: The name of the container in the ECS task definition.
            Defaults to 'zenml-container'.
        assign_public_ip: Whether to assign a public IP to the ECS tasks.
            Defaults to True.
    """

    synchronous: bool = True
    state_machine_type: str = DEFAULT_STATE_MACHINE_TYPE
    tags: Dict[str, str] = {}
    max_runtime_in_seconds: int = 86400  # 24 hours
    container_name: str = "zenml-container"
    assign_public_ip: bool = True


class StepFunctionsOrchestratorConfig(
    BaseOrchestratorConfig, StepFunctionsOrchestratorSettings
):
    """Config for the AWS Step Functions orchestrator.

    There are three ways to authenticate to AWS:
    - By connecting a `ServiceConnector` to the orchestrator,
    - By configuring explicit AWS credentials `aws_access_key_id`,
        `aws_secret_access_key`, and optional `aws_auth_role_arn`,
    - If none of the above are provided, unspecified credentials will be
        loaded from the default AWS config.

    Attributes:
        execution_role: The IAM role ARN to use for the state machine execution.
        ecs_cluster_arn: The ARN of the ECS cluster to run tasks in.
        ecs_task_definition_arn: The ARN of the ECS task definition to use.
        subnet_ids: List of subnet IDs where ECS tasks will run.
        security_group_ids: List of security group IDs for ECS tasks.
        aws_access_key_id: The AWS access key ID to use to authenticate to AWS.
        aws_secret_access_key: The AWS secret access key to use to authenticate
            to AWS.
        aws_profile: The AWS profile to use for authentication if not using
            service connectors or explicit credentials.
        aws_auth_role_arn: The ARN of an intermediate IAM role to assume when
            authenticating to AWS.
        region: The AWS region where the state machine will be created.
    """

    execution_role: str
    ecs_cluster_arn: str
    ecs_task_definition_arn: str
    subnet_ids: List[str]
    security_group_ids: List[str]
    aws_access_key_id: Optional[str] = SecretField(default=None)
    aws_secret_access_key: Optional[str] = SecretField(default=None)
    aws_profile: Optional[str] = None
    aws_auth_role_arn: Optional[str] = None
    region: Optional[str] = None

    @property
    def is_remote(self) -> bool:
        """Checks if this stack component is running remotely.

        Returns:
            True if this config is for a remote component, False otherwise.
        """
        return True

    @property
    def is_synchronous(self) -> bool:
        """Whether the orchestrator runs synchronous or not.

        Returns:
            Whether the orchestrator runs synchronous or not.
        """
        return self.synchronous


class StepFunctionsOrchestratorFlavor(BaseOrchestratorFlavor):
    """Flavor for the AWS Step Functions orchestrator."""

    @property
    def name(self) -> str:
        """Name of the flavor.

        Returns:
            The name of the flavor.
        """
        return AWS_STEP_FUNCTIONS_ORCHESTRATOR_FLAVOR

    @property
    def service_connector_requirements(
        self,
    ) -> Optional[ServiceConnectorRequirements]:
        """Service connector resource requirements for service connectors.

        Returns:
            Requirements for compatible service connectors.
        """
        return ServiceConnectorRequirements(resource_type=AWS_RESOURCE_TYPE)

    @property
    def docs_url(self) -> Optional[str]:
        """A url to point at docs explaining this flavor.

        Returns:
            A flavor docs url.
        """
        return self.generate_default_docs_url()

    @property
    def sdk_docs_url(self) -> Optional[str]:
        """A url to point at SDK docs explaining this flavor.

        Returns:
            A flavor SDK docs url.
        """
        return self.generate_default_sdk_docs_url()

    @property
    def logo_url(self) -> str:
        """A url to represent the flavor in the dashboard.

        Returns:
            The flavor logo.
        """
        return "https://public-flavor-logos.s3.eu-central-1.amazonaws.com/orchestrator/aws.png"

    @property
    def config_class(self) -> Type[StepFunctionsOrchestratorConfig]:
        """Returns StepFunctionsOrchestratorConfig config class.

        Returns:
            The config class.
        """
        return StepFunctionsOrchestratorConfig

    @property
    def implementation_class(self) -> Type["StepFunctionsOrchestrator"]:
        """Implementation class.

        Returns:
            The implementation class.
        """
        from orchestrator.my_aws_orchestrator import StepFunctionsOrchestrator

        return StepFunctionsOrchestrator
