import numpy as np
from sklearn.base import ClassifierMixin
from typing_extensions import Annotated
from zenml import step

@step
def evaluator(
    X_test: np.ndarray,
    y_test: np.ndarray,
    model: ClassifierMixin,
) -> Annotated[float, "test_accuracy"]:
    """Calculate the accuracy on the test set."""
    test_acc = model.score(X_test, y_test)
    print(f"Test accuracy: {test_acc}")
    return test_acc
