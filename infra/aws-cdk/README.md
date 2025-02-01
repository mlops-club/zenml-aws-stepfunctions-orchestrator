This is an AWS CDK project.

To make this work, I ran a one-time setup command to make infrastructure deployable with AWS CDK.

```bash
# only needs to be run once per account per region
bash run.sh cdk-bootstrap
```

To install AWS CDK, do this:

```bash
brew install node
npm install -g aws-cdk
```

To deploy this infrastructure, activate the AWS Profile mentioned in `run.sh` and run:

```bash
bash run.sh deploy
```

^^^ This assumes you have `uv` installed (`brew install uv`). This `uv` command
will ensure the proper python dependencies are installed and then run deploy the infrastructure.

```bash
# Run this to install the necessary dependencies
uv sync
```
