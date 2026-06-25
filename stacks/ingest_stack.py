"""
Create S3 bucket + Size-tracking lambda + Trigger from S3 -> lambda

An S3 event notification needs the function ARN, and the function needs the
bucket name -- splitting them across stacks creates a circular dependency
("deadly embrace"). Keeping them together side-steps that entirely.
"""

import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from constructs import Construct


class IngestStack(Stack):
    SIZE_TRACKING_FN_ID = "SizeTrackingFn"
    BUCKET_CONSTRUCT_ID = "TestBucket"
    SIZE_TRACKING_CODE_DIR = "lambdas/size_tracker"

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        table: dynamodb.ITable,
        gsi_partition_value: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # No bucket_name -> CloudFormation generates a globally-unique one,
        # replacing the f"s3-size-tracker-{ACCOUNT_ID}" trick in main.py.
        self.bucket = s3.Bucket(
            self,
            self.BUCKET_CONSTRUCT_ID,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,  # so `cdk destroy` empties + drops it
        )

        size_fn = lambda_.Function(
            self,
            self.SIZE_TRACKING_FN_ID,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(self.SIZE_TRACKING_CODE_DIR),
            timeout=Duration.seconds(30),
            # CloudFormation-owned log group so `cdk destroy` removes it too;
            # without this, Lambda creates an orphan /aws/lambda/... group.
            log_group=logs.LogGroup(
                self,
                f"{self.SIZE_TRACKING_FN_ID}Logs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            ),
            environment={
                "TABLE_NAME": table.table_name,
                "GSI_PARTITION_VALUE": gsi_partition_value,
            },
        )

        # Least-privilege replacement for the manual customer-managed policy.
        self.bucket.grant_read(size_fn)  # ListBucket + GetObject
        table.grant_write_data(size_fn)  # PutItem

        # OBJECT_CREATED covers put/post/copy/multipart (create + update);
        # OBJECT_REMOVED covers deletes -> "all object creation/update/delete".
        notify = s3n.LambdaDestination(size_fn)
        self.bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notify)
        self.bucket.add_event_notification(s3.EventType.OBJECT_REMOVED, notify)

        CfnOutput(self, "BucketName", value=self.bucket.bucket_name)
