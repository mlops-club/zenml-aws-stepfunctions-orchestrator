"""
## A simple orchestrator

Very simple orchestrator to show a pipeline running on Docker
"""

from .my_docker_orchestrator import (
    LocalDockerOrchestrator,
    LocalDockerOrchestratorConfig,
    LocalDockerOrchestratorSettings,
)
from .my_docker_orchestrator_flavor import LocalDockerOrchestratorFlavor

__all__ = [
    "LocalDockerOrchestrator",
    "LocalDockerOrchestratorConfig",
    "LocalDockerOrchestratorSettings",
    "LocalDockerOrchestratorFlavor",
]
