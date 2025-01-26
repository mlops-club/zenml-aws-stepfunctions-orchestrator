# 🎼 Create a Custom Orchestrator with ZenML

ZenML allows you to [create a custom orchestrator](https://docs.zenml.io/stack-components/orchestrators/custom), an essential component in any MLOps stack responsible for running your machine learning pipelines. This tutorial guides you through the process of creating an orchestrator that runs each step of a pipeline locally in a docker container.

## ❓Why would you need a custom orchestrator?

While ZenML comes built with standard integrations for well-known orchestrators like [Airflow](https://docs.zenml.io/stack-components/orchestrators/airflow), [Kubeflow](https://docs.zenml.io/stack-components/orchestrators/kubeflow), and even running [locally](https://docs.zenml.io/stack-components/orchestrators/local), your business might either want to orchestrate your ML workloads differently or slightly tweak the implementations of the standard orchestrators. In this case, this guide is useful, as it implements a relatively simple orchestrator.

The `BaseOrchestrator` abstracts away many of the ZenML-specific details from the actual implementation and exposes a simplified interface. This example is an end-to-end guide on creating a custom orchestrator using ZenML. Click [here](https://docs.zenml.io/stack-components/orchestrators/custom) to learn more about the custom orchestrator interface.

## 💻 Tutorial: Creating a custom orchestrator that runs each step in a docker container

### 📑 Prerequisites

To run this example, you need to have ZenML locally installed along with the necessary dependencies. You can do so by executing the following commands:

```shell
# clone the repository
git clone ...

# install the necessary dependencies
pip install -r requirements.txt
```

Also, you need to have a deployed ZenML server. You can do so by following the instructions [here](https://docs.zenml.io/getting-started/deploying-zenml) or by creating a free account on [ZenML Pro](https://cloud.zenml.io/). Assuming you have a ZenML server, you can run the following command to connect to it:

```shell
# connect to the ZenML server
zenml login <your-zenml-server-url>  # just use `zenml login` if you are using ZenML Pro
```

Please note that you also need to have Docker installed on your machine.

### 🚀 Registering a simple Custom Orchestrator: The `MyDockerOrchestrator`

In order to learn how to create a custom orchestrator, we will start with a simple one. This orchestrator will run each step of the pipeline in a docker container. This orchestrator is a good one to start because it can be coupled with a local artifact store, and
therefore can be run locally.

First, you need to initialize zenml at the root of the repository:

```shell
# initialize zenml at the root of the repository
zenml init
```

Then, you need to register the flavor of the orchestrator:

```shell
# register the flavor of the orchestrator
zenml orchestrator flavor register orchestrator.my_docker_orchestrator_flavor.LocalDockerOrchestratorFlavor
```

Then, you register your custom orchestrator using your registered flavor:

```shell
# register the custom orchestrator
zenml orchestrator register my_docker_orchestrator -f my_docker  
```

Note in this case, the registration of the orchestrator has no settings, as the orchestrator is not using any
config or settings. Your orchestrator might have more complicated config that you can set here.

### 📝 Registering and Setting the Stack

Next, you need to register a stack with your custom orchestrator and the default artifact store attached:

```shell
# register the stack
zenml stack register my_stack -o my_docker_orchestrator -a default
```

Finally, set the stack active. This means every pipeline that runs will use the custom orchestrator:

```shell
# set the stack active
zenml stack set my_stack
```

### 📝 Running a Pipeline and Testing the Orchestrator

This example ships with a simple pipeline that runs a `sklearn` pipeline. You can run it by executing the following command:

```shell
# run the pipeline
python run.py
```

By default, the pipeline is configured at runtime with the config file `configs/training_pipeline.yaml`.
When testing the orchestrator, you might want to check certain features like resource and orchestrator settings. You can do so by editing the config file:

```yaml
settings:
  docker:
    required_integrations:
      - sklearn
    requirements:
      - pyarrow
  orchestrator:
    some_setting: "some_value"

resources:
  cpu_count: 4
  memory: "4Gb"
```

## Start developing the AWS Step Orchestrator

Now that you have a working custom orchestrator, you can start developing the AWS Step Orchestrator.
In principle, the process is the same. You need to create a new flavor, register it, then register the orchestrator, and
finally register the stack with the orchestrator.

However, there are some nuances that you need to be aware of:

1. The AWS Step Orchestrator is a bit more complex than the Local Docker Orchestrator, as it requires a few more components to be set up. As the pipeline will run remotely, the stack needs to have an AWS artifact store and AWS container registry.
2. Ideally, the AWS Step Orchestrator will require to set up a [AWS Service Connector](https://docs.zenml.io/how-to/infrastructure-deployment/auth-management/aws-service-connector) in order to authenticate your local machine to AWS services. This is totally optional, but it is a good practice to do so.

Here is a simple example how you would set up a AWS Step Orchestrator:

```shell
# install the necessary integrations
zenml integration install aws s3 -y
```

```shell
# register the flavor
zenml orchestrator flavor register orchestrator.my_aws_orchestrator_flavor.AWSStepOrchestratorFlavor
```

```shell
# register the orchestrator
zenml orchestrator register my_aws_orchestrator -f my_aws
```

```shell
# Register a AWS Service Connector
# Note: This will configure the connector to use the AWS profile you have set in your environment
zenml service-connector register cloud_connector --type aws --auto-configure
```

```shell
# Register the S3 artifact-store and connect it to the AWS Service Connector
zenml artifact-store register cloud_artifact_store -f s3 --path=s3://bucket-name
zenml artifact-store connect cloud_artifact_store --connector cloud_connector
```

```shell
# Register the container registry and connect it to the AWS Service Connector
zenml container-registry register cloud_container_registry -f aws --uri=<ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com
zenml container-registry connect cloud_container_registry --connector cloud_connector
```

```shell
# register the stack
zenml stack register my_aws_stack -o my_aws_orchestrator -a cloud_artifact_store -c cloud_container_registry
```

```shell
# set the stack active
zenml stack set my_aws_stack
```

Now you can run the pipeline by executing the following command:

```shell
# run the pipeline
python run.py
```

And that's it! If you implemented the AWS Step Orchestrator correctly, the pipeline should run remotely in AWS on AWS Step Functions.

## 📚 Learn More

For more information on creating a custom orchestrator in ZenML, follow this [guide](https://docs.zenml.io/stack-components/orchestrators/custom).
