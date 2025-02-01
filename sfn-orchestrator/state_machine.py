import time
import boto3
import json

# Define AWS region
AWS_REGION = "us-west-2"  # Change to your region

# Step Functions client
sfn_client = boto3.client("stepfunctions", region_name=AWS_REGION)

# IAM Role for Step Functions
STEP_FUNCTIONS_ROLE_ARN = "arn:aws:iam::847068433460:role/zenml-hackathon-step-functions-role"  # Replace with your IAM role ARN

# Define the state machine JSON
state_machine_definition = {
  "Comment": "A state machine to submit the AWS Batch job from Python",
  "StartAt": "Submit AWS Batch Job from Python",
  "States": {
    "Submit AWS Batch Job from Python": {   # This should match the StartAt value
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Parameters": {
        "JobDefinition": "zenml-test",  # Change if needed
        "JobQueue": "zenml-fargate-queue-manual",  # Change if needed
        "JobName": "zenml-fargate-job-sfn-from-python-script"
      },
      "End": True
    }
  }
}

#   "Arguments": {
    #     "JobName": "zenml-fargate-job-sfn-from-python-script",
    #     "JobDefinition": "arn:aws:batch:us-west-2:847068433460:job-definition/zenml-test:2",
    #     "JobQueue": "arn:aws:batch:us-west-2:847068433460:job-queue/zenml-fargate-queue-manual"
    #   },




# Convert to JSON
state_machine_json = json.dumps(state_machine_definition)

# Create the state machine
response = sfn_client.create_state_machine(
    name="ZenML_Batch_Job_StateMachine_Python_Script",
    definition=state_machine_json,
    roleArn=STEP_FUNCTIONS_ROLE_ARN,
    type="STANDARD"  # Change to "EXPRESS" if needed
)

print(f"State Machine ARN: {response['stateMachineArn']}")


STATE_MACHINE_ARN = response["stateMachineArn"]
print(f"✅ State Machine Created: {STATE_MACHINE_ARN}")


# ✅ 2️⃣ Start the Execution of the State Machine
response = sfn_client.start_execution(
    stateMachineArn=STATE_MACHINE_ARN,
    name="ZenML_Batch_Job_Execution",
    input="{}"  # Empty input, modify if needed
)

EXECUTION_ARN = response["executionArn"]
print(f"🚀 Step Function Execution Started: {EXECUTION_ARN}")


# ✅ 3️⃣ Monitor Execution Progress
def check_execution_status(execution_arn):
    while True:
        response = sfn_client.describe_execution(executionArn=execution_arn)
        status = response["status"]
        print(f"🔄 Execution Status: {status}")

        if status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
            print(f"✅ Execution Completed with status: {status}")
            break

        time.sleep(10)  # Poll every 5 seconds


check_execution_status(EXECUTION_ARN)