import numpy as np
from sklearn.base import ClassifierMixin
from sklearn.svm import SVC
from typing_extensions import Annotated
from zenml import step

@step
def trainer(
    X_train: np.ndarray,
    y_train: np.ndarray,
) -> Annotated[ClassifierMixin, "trained_model"]:
    """Train a simple sklearn classifier for the digits dataset."""
    model = SVC(gamma=0.001)
    model.fit(X_train, y_train)
    return model
