import aws_cdk as cdk
from aws_cdk import Stack
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_iam as iam
from constructs import Construct


class AWSBatchStack(Stack):
    """
    AWS Batch Compute Environment using ECS as Compute Resource
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Default VPC
        vpc: ec2.IVpc = ec2.Vpc.from_lookup(
            scope=self, id="AwsBatchVpc", vpc_name="local-oregon"
        )

        # enable all outbound, and no inbound
        batch_security_group = ec2.SecurityGroup(
            self,
            "BatchSecurityGroup",
            vpc=vpc,
            security_group_name="zenml-hackathon-batch-security-group",
            description="Security Group for AWS Batch Compute Environment",
            allow_all_outbound=True,  # Allow all outbound traffic
        )

        # Create an ECS cluster for the Batch Compute Environment
        ecs_cluster: ecs.Cluster = ecs.Cluster(
            self,
            id="ZenMLBatchECSCluster",
            cluster_name="zenml-hackathon-batch-ecs-cluster",
            vpc=vpc,
        )

        # IAM Role for AWS Batch Jobs
        batch_job_role: iam.Role = iam.Role(
            self,
            id="ZenMLBatchJobRole",
            role_name="zenml-hackathon-batch-job-role",
            # https://gist.github.com/shortjared/4c1e3fe52bdfa47522cfe5b41e5d6f22
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal(
                    "ecs-tasks.amazonaws.com"
                ),  # Allows ECS tasks to assume this role
                iam.ServicePrincipal(
                    "batch.amazonaws.com"
                ),  # Allows Batch jobs to assume this role
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSBatchServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonEC2ContainerRegistryReadOnly"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchLogsFullAccess"
                ),
            ],
        )

        # AWS Batch Compute Environment
        compute_env = batch.ManagedEc2EcsComputeEnvironment(
            self,
            id="ZenMLECSEnvironment",
            compute_environment_name="zenml-hackathon-ecs-compute-environment",
            # instance_types=[ec2.InstanceType.of(ec2.InstanceClass.M4, ec2.InstanceSize.LARGE)],
            instance_classes=[ec2.InstanceClass.M4],
            use_optimal_instance_classes=True,
            allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
            maxv_cpus=2,
            vpc=vpc,
            security_groups=[batch_security_group],
            service_role=batch_job_role,
        )

        # Job Queue
        job_queue: batch.JobQueue = batch.JobQueue(
            self,
            id="ZenMLECSBatchJobQueue",
            job_queue_name="zenml-hackathon-ecs-batch-job-queue",
            compute_environments=[
                batch.OrderedComputeEnvironment(
                    order=1, compute_environment=compute_env
                ),
            ],
            priority=1,
        )

        # Outputs
        cdk.CfnOutput(self, "ecs-cluster-name", value=ecs_cluster.cluster_name)
        cdk.CfnOutput(self, "batch-job-queue-name", value=job_queue.job_queue_name)
