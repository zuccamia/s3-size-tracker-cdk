"""
Event source for the app: S3 bucket + SNS fanout topic + size-tracking consumer.

Wiring:

    S3 bucket -> SNS S3EventsTopic -> SQS SizeTrackerQueue -> SizeTrackingFn
                                          (DLQ after 5 receives)

Additional consumers (the logger + alarm + cleaner control loop) live in
AutoCleanupStack, subscribed to the same topic. Anything else that needs S3
events -- future tail-logger, replicator, whatever -- would similarly attach
to `self.topic` from a separate stack.

An S3 event notification needs the topic/lambda ARN, and the function needs
the bucket name -- splitting these across stacks creates a circular dependency
("deadly embrace"). Keeping the S3->SNS wiring in the same stack as the bucket
side-steps that. Downstream consumers only reference the topic (unidirectional
dep), which is safe.
"""

import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_lambda_event_sources as lambda_events
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subscriptions as sns_subs
import aws_cdk.aws_sqs as sqs
from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from constructs import Construct


class IngestStack(Stack):
    SIZE_TRACKING_FN_ID = "SizeTrackingFn"
    SIZE_TRACKING_CODE_DIR = "lambdas/size_tracker"
    BUCKET_CONSTRUCT_ID = "TestBucket"
    TOPIC_ID = "S3EventsTopic"
    QUEUE_ID = "SizeTrackerQueue"
    DLQ_ID = "SizeTrackerDLQ"
    S3_EVENTS_LAYER_ID = "S3EventsLayer"
    S3_EVENTS_LAYER_DIR = "layers/s3_events"

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

        # Fanout hub: S3 publishes here, any number of subscribers can consume.
        self.topic = sns.Topic(self, self.TOPIC_ID)

        # Shared envelope-parsing helper (s3_events.py). Exposed on self so
        # downstream stacks (AutoCleanupStack) can attach it to their own
        # SQS-triggered consumers without duplicating the two-JSON-loads peel.
        self.s3_events_layer = lambda_.LayerVersion(
            self,
            self.S3_EVENTS_LAYER_ID,
            code=lambda_.Code.from_asset(self.S3_EVENTS_LAYER_DIR),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
        )

        # DLQ retention outlives the main queue's so we have time to inspect
        # poison messages after they stop being retried.
        self.dlq = sqs.Queue(
            self,
            self.DLQ_ID,
            retention_period=Duration.days(14),
        )
        self.queue = sqs.Queue(
            self,
            self.QUEUE_ID,
            # Give the lambda room to recompute large buckets; must be >= fn
            # timeout, otherwise SQS re-delivers a message that's still in-flight.
            visibility_timeout=Duration.seconds(60),
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=self.dlq,
                max_receive_count=5,
            ),
        )

        # S3 -> SNS: OBJECT_CREATED covers put/post/copy/multipart,
        # OBJECT_REMOVED covers deletes -> "all object create/update/delete".
        notify = s3n.SnsDestination(self.topic)
        self.bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notify)
        self.bucket.add_event_notification(s3.EventType.OBJECT_REMOVED, notify)

        # SNS -> SQS. Raw delivery OFF so the lambda still sees the SNS envelope
        # (Type/Message/...) it now parses; flipping this to True would strip
        # the envelope and require another parser change.
        self.topic.add_subscription(sns_subs.SqsSubscription(self.queue))

        size_fn = lambda_.Function(
            self,
            self.SIZE_TRACKING_FN_ID,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(self.SIZE_TRACKING_CODE_DIR),
            layers=[self.s3_events_layer],
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

        # SQS -> Lambda. report_batch_item_failures lets the handler return
        # {"batchItemFailures": [...]} so only the bad messages get retried
        # (and eventually DLQ'd) instead of the whole batch.
        size_fn.add_event_source(
            lambda_events.SqsEventSource(
                self.queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        CfnOutput(self, "BucketName", value=self.bucket.bucket_name)
        CfnOutput(self, "TopicArn", value=self.topic.topic_arn)
        CfnOutput(self, "SizeTrackerQueueUrl", value=self.queue.queue_url)
        CfnOutput(self, "SizeTrackerDLQUrl", value=self.dlq.queue_url)
