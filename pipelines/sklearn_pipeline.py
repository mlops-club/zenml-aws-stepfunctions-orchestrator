from steps import digits_data_loader, evaluator, trainer

from zenml import pipeline


@pipeline
def sklearn_pipeline():
    """Links all the steps together in a pipeline."""
    X_train, X_test, y_train, y_test = digits_data_loader()
    model = trainer(X_train=X_train, y_train=y_train)
    evaluator(X_test=X_test, y_test=y_test, model=model)
