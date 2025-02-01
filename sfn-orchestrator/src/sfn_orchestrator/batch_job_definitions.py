import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import boto3
from rich import inspect, print
from zenml.config.resource_settings import ByteUnit, ResourceSettings
from zenml.entrypoints import StepEntrypointConfiguration
from zenml.models import PipelineDeploymentResponse


@dataclass
class BatchJobDefinitionConfig:
    """Configuration for an AWS Batch job definition."""

    step_name: str
    image: str
    environment: Dict[str, str]
    command: List[str]
    cpu: int
    memory: int
    gpu: int = 0

    @property
    def hashed_name(self) -> str:
        """Generate the job definition name from step name and config hash."""
        return f"zenml-{self.step_name}-{self.to_hash()[:8]}"

    @classmethod
    def from_deployment(
        cls,
        step_name: str,
        deployment: PipelineDeploymentResponse,
        environment: Dict[str, str],
        get_image_fn: Callable[[PipelineDeploymentResponse, str], str],
    ) -> "BatchJobDefinitionConfig":
        """Create a BatchJobDefinitionConfig from a ZenML pipeline step.

        Args:
            step_name: Name of the pipeline step
            deployment: ZenML pipeline deployment
            environment: Environment variables for all steps
            get_image_fn: Function to get Docker image for a step

        Returns:
            BatchJobDefinitionConfig for the step
        """
        step_config = deployment.step_configurations[step_name]

        resource = ResourceSettings(
            # TODO: better declare these defaults somewhere else
            # TODO: these defaults probably don't work, we should probably
            #       first look for step-level settings, then pipeline-level settings, then defaults
            cpu_count=step_config.config.resource_settings.cpu_count or 1024,
            memory=step_config.config.resource_settings.memory or "2048MB",
            gpu_count=step_config.config.resource_settings.gpu_count or 0,
        )

        print(resource)
        inspect(resource)

        # Validate resources
        _validate_fargate_resources(
            resource.cpu_count,
            resource.get_memory(unit=ByteUnit.MB),
            resource.gpu_count,
        )

        # Get step command
        command = StepEntrypointConfiguration.get_entrypoint_command()
        arguments = StepEntrypointConfiguration.get_entrypoint_arguments(
            step_name=step_name,
            deployment_id=deployment.id,
        )

        return cls(
            step_name=step_name,
            image=get_image_fn(deployment, step_name),
            environment=sorted(environment.items()),
            command=command + arguments,
            cpu=resource.cpu_count,
            memory=resource.get_memory(unit=ByteUnit.MB),
            gpu=resource.gpu_count,
        )

    def to_hash(self) -> str:
        """Generate a hash of the configuration.

        Returns:
            SHA-256 hash of the configuration
        """
        config = {
            "image": self.image,
            "environment": sorted(self.environment),
            "command": self.command,
            "cpu": self.cpu,
            "memory": self.memory,
            "gpu": self.gpu,
        }
        return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


######################
# --- With boto3 --- #
######################


def register_equivalent_job_definitions_for_pipeline(
    batch_client: "boto3.client",
    execution_role_arn: str,
    deployment: PipelineDeploymentResponse,
    environment: Dict[str, str],
    get_image_fn: Callable[[PipelineDeploymentResponse, str], str],
    task_role_arn: Optional[str] = None,
) -> Dict[str, str]:
    """Register AWS Batch job definitions for pipeline steps and return mapping."""
    job_def_configs: List[BatchJobDefinitionConfig] = (
        _get_job_def_configs_from_deployment_pipeline(
            deployment, environment, get_image_fn
        )
    )

    return _register_equivalent_job_definitions_for_pipeline(
        batch_client=batch_client,
        execution_role_arn=execution_role_arn,
        job_def_configs=job_def_configs,
        task_role_arn=task_role_arn,
    )


def _register_equivalent_job_definitions_for_pipeline(
    batch_client: "boto3.client",
    execution_role_arn: str,
    job_def_configs: List[BatchJobDefinitionConfig],
    task_role_arn: Optional[str] = None,
) -> Dict[str, str]:
    """Register AWS Batch job definitions for pipeline steps and return mapping."""

    # Remove duplicates based on whether the job definition data is equivalent to others
    names_to_job_def_configs: Dict[str, BatchJobDefinitionConfig] = (
        _remove_duplicate_job_def_configs(job_def_configs)
    )

    # Register each unique job definition
    for job_def_name, job_def_config in names_to_job_def_configs.items():
        try:
            batch_client.register_job_definition(
                **_generate_job_definition_config(
                    job_definition_name=job_def_name,
                    **job_def_config.to_dict(),
                    execution_role_arn=execution_role_arn,
                    task_role_arn=task_role_arn,
                )
            )
        except Exception as e:
            raise RuntimeError(f"Failed to register job definition: {e}")

    # Create mapping of steps to job definition names
    step_to_job_def = {
        config.step_name: config.hashed_name for config in job_def_configs
    }

    return step_to_job_def


#########################
# --- Without boto3 --- #
#########################


def get_aws_batch_job_definition_settings_from_deployment(
    deployment: PipelineDeploymentResponse,
    environment: Dict[str, str],
    get_image_fn: Callable[[PipelineDeploymentResponse, str], str],
) -> List[BatchJobDefinitionConfig]:
    """Get job definition configs for all steps in a pipeline."""
    # Get configs for all steps
    job_def_configs: List[BatchJobDefinitionConfig] = (
        _get_job_def_configs_from_deployment_pipeline(
            deployment, environment, get_image_fn
        )
    )
    return job_def_configs


def _get_job_def_configs_from_deployment_pipeline(
    deployment: PipelineDeploymentResponse,
    environment: Dict[str, str],
    get_image_fn: Callable[[PipelineDeploymentResponse, str], str],
) -> List[BatchJobDefinitionConfig]:
    """Get job definition configs for all steps in a pipeline.

    Args:
        deployment: ZenML pipeline deployment
        environment: Environment variables for all steps
        get_image_fn: Function to get Docker image for a step

    Returns:
        List of job definition configs for all steps
    """
    return [
        BatchJobDefinitionConfig.from_deployment(
            step_name=step_name,
            deployment=deployment,
            environment=environment,
            get_image_fn=get_image_fn,
        )
        for step_name in deployment.step_configurations
    ]


def _remove_duplicate_job_def_configs(
    configs: List[BatchJobDefinitionConfig],
) -> Dict[str, BatchJobDefinitionConfig]:
    """Remove duplicate job definition configs based on their hashed names.

    Args:
        configs: List of job definition configs

    Returns:
        Dict mapping unique hashed names to their configs
    """
    return {
        config.hashed_name: config
        for config in configs
        if config.hashed_name
        not in {c.hashed_name for c in configs[: configs.index(config)]}
    }


def _validate_fargate_resources(cpu: int, memory: int, gpu: int = 0) -> None:
    """Validate CPU and memory values for Fargate."""
    valid_cpu = {256, 512, 1024, 2048, 4096, 8192, 16384}
    valid_memory = {
        256: {512, 1024, 2048},
        512: {1024, 2048, 3072, 4096},
        1024: set(range(2048, 8193, 1024)),
        2048: set(range(4096, 16385, 1024)),
        4096: set(range(8192, 30720, 1024)),
        8192: set(range(16384, 61440, 1024)),
    }

    if gpu:
        raise ValueError(
            "GPUs are not supported, because the ZenML Step Functions orchestrator currently only supports ECS Fargate"
        )

    if cpu not in valid_cpu:
        raise ValueError(
            f"Invalid CPU value {cpu}. Must be one of: {sorted(valid_cpu)}"
        )

    if memory not in valid_memory.get(cpu, set()):
        raise ValueError(
            f"Invalid memory value {memory} for CPU {cpu}. "
            f"Valid values: {sorted(valid_memory[cpu])}"
        )


def _generate_job_definition_config(
    job_definition_name: str,
    image: str,
    execution_role_arn: str,
    cpu: int,
    memory: int,
    environment: Optional[Dict[str, str]] = None,
    command: Optional[List[str]] = None,
    task_role_arn: Optional[str] = None,
    gpu: int = 0,
) -> Dict[str, Any]:
    """Generate AWS Batch job definition configuration.

    Args:
        job_definition_name: Name for the job definition
        image: Docker image URI
        execution_role_arn: ARN of the execution role
        cpu: CPU units (1024 = 1 vCPU)
        memory: Memory in MiB
        environment: Optional environment variables
        command: Optional command override
        task_role_arn: Optional task role ARN
        gpu: Number of GPUs (default 0)

    Returns:
        Job definition configuration dictionary
    """
    container_properties = {
        "image": image,
        "executionRoleArn": execution_role_arn,
        "networkConfiguration": {"assignPublicIp": "ENABLED"},
        "fargatePlatformConfiguration": {"platformVersion": "LATEST"},
        "resourceRequirements": [
            {"type": "VCPU", "value": str(cpu)},
            {"type": "MEMORY", "value": str(memory)},
        ],
    }

    # Add optional properties
    if environment:
        container_properties["environment"] = [
            {"name": k, "value": v} for k, v in environment
        ]
    if command:
        container_properties["command"] = command
    if task_role_arn:
        container_properties["jobRoleArn"] = task_role_arn
    if gpu > 0:
        container_properties["resourceRequirements"].append(
            {"type": "GPU", "value": str(gpu)}
        )

    return {
        "jobDefinitionName": job_definition_name,
        "type": "container",
        "containerProperties": container_properties,
        "platformCapabilities": ["FARGATE"],
    }
