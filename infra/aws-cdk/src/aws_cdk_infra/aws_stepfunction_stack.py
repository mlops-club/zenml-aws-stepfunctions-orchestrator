import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam
)
from constructs import Construct

# aws batch compute environment (our step functions state machines will deploy containers into this)
class AWSStepFunctionStack(Stack):
    """
    AWS Step Function to Run AWS Batch job 
    """
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        
        super().__init__(scope, construct_id, **kwargs)
        
        # Import job queue arn and job definition arn, to be used for aws step function
        job_queue_arn = cdk.Fn.import_value("zenml-hackathon-ecs-batch-job-queue-arn")
        job_definition_arn = cdk.Fn.import_value("zenml-hackathon-ecs-batch-job-definition-arn")
        ecrImage_uri = cdk.Fn.import_value("zenml-hackathon-ecr-repo-image-uri")
        
        
        # TODO: IAM roles that allow step function to invoke the AWS batch job
        step_function_iam_role: iam.Role = iam.Role(
            self,
            "step_function_iam_role",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            description="Step Function IAM Role",
        )
        # First policy to allow step function to invoke batch job
        step_function_iam_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "batch:SubmitJob",
                    "batch:DescribeJobs",
                    "batch:TerminateJob",
                ],
                resources=["*"],
            )
        )
        
        #TODO: Define the state machine to start state 
        start_state = sfn.Pass(self, 
                               "Start State",
                               result_path=sfn.Result.from_object({"message: Starting the workflow"}),
                               output_path="$.startResult")

        submit_job_state = tasks.BatchSubmitJob(self, "Submit Job",
                job_definition_arn=job_definition_arn,
                job_name = "zenml-hackathon-submit-batch-job",
                job_queue_arn=job_queue_arn,
                container_overrides={
                     "vcpus": 1,
                     "memory": 1024,
                     "image": ecrImage_uri,
                },
                result_path=sfn.Result.from_object({"message: Submitting the job"}),
                output_path="$.submitResult"
)
        # TODO: wait state
        wait_state = sfn.Wait(self, "Wait State",
                              time=sfn.WaitTime.duration(cdk.Duration.seconds(60)),
                              output_path="$.waitResult")
        
        # TODO: succeed state and failed state
        success_state = sfn.Succeed(self, "Success", comment="Job completed successfully")
        failure_state = sfn.Fail(self, "Failure", error="JobFailed", cause="The job did not complete successfully.")
        
        
        #TODO: Workflow definition can be defined with next()
        start_state.next(submit_job_state)
        submit_job_state.next(wait_state)
        wait_state.next(success_state)
        wait_state.next(failure_state)
        
        
        #TODO: Finally, create the state machine
        state_machine = sfn.StateMachine(
            self,
            "ZenMLHackathonStateMachine",
            stateMachineName="zenml-sfn-state-machine",
            role=step_function_iam_role,
            definition = start_state,
            
        )
        
        # Output the state machine ARN
        cdk.CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        
                
        