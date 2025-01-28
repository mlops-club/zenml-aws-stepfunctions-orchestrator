"""AWS Step Functions orchestrator flavor."""

from typing import TYPE_CHECKING, Dict, List, Optional, Type

from zenml.config.base_settings import BaseSettings
from zenml.config.base_config import BaseConfig
from zenml.integrations.aws import AWS_RESOURCE_TYPE
from zenml.models import ServiceConnectorRequirements
from zenml.orchestrators import BaseOrchestratorConfig
from zenml.orchestrators.base_orchestrator import BaseOrchestratorFlavor

if TYPE_CHECKING:
    from orchestrator.my_aws_orchestrator import StepFunctionsOrchestrator

DEFAULT_STATE_MACHINE_TYPE = "STANDARD"
AWS_STEP_FUNCTIONS_ORCHESTRATOR_FLAVOR = "aws_step_functions"


class StepFunctionsOrchestratorSettings(BaseSettings):
    """Settings for the AWS Step Functions Orchestrator.

    Attributes:
        container_name: Name of the container in task definition
        assign_public_ip: Whether to assign public IP to tasks
        max_runtime_in_seconds: Maximum runtime for tasks
        state_machine_type: Type of state machine (STANDARD/EXPRESS)
        tags: Custom tags to apply to resources
        synchronous: Whether to wait for pipeline completion
    """

    container_name: str = "zenml"
    state_machine_type: str = "STANDARD"
    network_mode: str = "awsvpc"
    requires_compatibilities: List[str] = ["FARGATE"]
    tags: Dict[str, str] = {}
    synchronous: bool = True


class StepFunctionsOrchestratorConfig(
    BaseOrchestratorConfig, StepFunctionsOrchestratorSettings
):
    """Configuration for the AWS Step Functions Orchestrator.

    Attributes:
        ecs_cluster_arn: ARN of ECS cluster
        execution_role: ARN of execution role
        task_role: ARN of task role
        subnet_ids: List of subnet IDs
        security_group_ids: List of security group IDs
        region: AWS region
    """

    ecs_cluster_arn: str
    execution_role: str
    task_role: str
    subnet_ids: List[str]
    security_group_ids: List[str]
    region: str

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
