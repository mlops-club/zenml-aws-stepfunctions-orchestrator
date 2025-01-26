"""
## Exemplary Steps

Three very simple steps
"""
from .evaluator.evaluator_step import evaluator
from .importer.importer_step import digits_data_loader
from .trainer.trainer_step import trainer

__all__ = ["digits_data_loader", "trainer", "evaluator"]
