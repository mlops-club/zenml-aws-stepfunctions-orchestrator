#!/bin/bash


AWS_PROFILE=pattern         # Using `Sandbox` account
AWS_DEFAULT_REGION=us-west-2


export THIS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
export JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1


# Deploy your stack
function deploy {
    uv run -- \
        cdk deploy --all \
        --profile $AWS_PROFILE \
        --region $AWS_DEFAULT_REGION
}


# Delete your CDK stack
function cdk-destroy {
    uv run -- cdk destroy --profile $AWS_PROFILE
}


# Bootstrap your CDK environment
function cdk-bootstrap {
    uv run -- cdk bootstrap --profile $AWS_PROFILE
}

function help {
    echo "$0 <task> <args>"
    echo "Tasks:"
    compgen -A function | cat -n
}

TIMEFORMAT="Task completed in %3lR"
time ${@:-default}
