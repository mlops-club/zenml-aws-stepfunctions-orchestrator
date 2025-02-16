# ZenML Plugin: Step Functions Orchestrator

```bash
# Install the all packages with their dependencies
uv sync --all-packages
```

## Resources

- reference [terraform template](https://github.com/zenml-io/terraform-aws-zenml-stack/blob/main/main.tf)
- [pulumi/terraform](https://www.pulumi.com/blog/any-terraform-provider/)

## ./Running this from scratch

```bash
# system deps
brew install pulumi uv node

# aws cdk
npm install -g aws-cdk@latest

# python deps
bash ./run install

# cdk infra
bash ./run cdk-deploy

# register the orchestrator
cd orchestrator
uv sync
uvx zenml init
uv run zenml orchestrator flavor register sfn_orchestrator.sfn_orchestrator_flavor.StepFunctionsOrchestratorFlavor
# uv zenml orchestrator register aws_step_functions --flavor aws_step_functions
cd ..

# pulumi infra
pulumi config set aws:profile sbox
pulumi config set aws:profile us-west-2
pulumi config set zenml:serverUrl https://<tenant>.cloudinfra.zenml.io
# pulumi config set --secret zenml:apiKey eyJp... # service connector
bash ./run pulumi-login-local
bash ./run pulumi-up

# Set the stack
uv run -- zenml stack set sfn-s3-ecr

# run the pipeline 🎉
uv run simple_pipeline.py
```
