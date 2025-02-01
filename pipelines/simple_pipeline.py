from zenml import pipeline, step
from zenml.config.resource_settings import ResourceSettings
from zenml.steps.entrypoint_function_utils import StepArtifact


@step
def step_1(num: int) -> int:
    print(f"Step 1: {num}")
    return num


@step
def multiply_by_two(num: int) -> int:
    print(f"Multiplying {num} by 2")
    return num * 2


@step
def add_three(num: int) -> int:
    print(f"Adding 3 to {num}")
    return num + 3


@step(
    settings={
        "resources": ResourceSettings(
            cpu_count=512,
            memory="1024MB",
        ),
    }
)
def add_artifacts(num1: int, num2: int) -> int:
    result = num1 + num2
    print(f"Result: {result}")
    return result


# TODO, how do you add two artifacts? (probably in another step)
@pipeline
def simple_pipeline(num: int):
    num = step_1(num)
    o1: StepArtifact = multiply_by_two(num)
    o2: StepArtifact = add_three(num)
    o3: StepArtifact = add_artifacts(o1, o2)


if __name__ == "__main__":
    simple_pipeline.with_options(
        enable_cache=False,
    )(num=100)
