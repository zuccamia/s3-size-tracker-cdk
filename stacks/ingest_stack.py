"""
Create S3 bucket + SNS topic + per-consumer SQS queues (with DLQs) + consumer
lambdas (size tracker + logger).

Fanout wiring:

    S3 -> SNS topic -+-> SQS SizeTrackerQueue -> SizeTrackingFn
                     |         (DLQ after 5 receives)
                     |
                     +-> SQS LoggingQueue     -> LoggingFn
                               (DLQ after 5 receives)

Both consumers share a small "s3_events" Lambda layer that peels the
SQS -> SNS -> S3 envelope; a third consumer would just be a new queue +
subscription + function.

An S3 event notification needs the topic/lambda ARN, and the function needs the
bucket name -- splitting them across stacks creates a circular dependency
("deadly embrace"). Keeping them together side-steps that entirely.
"""

import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_iam as iam
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
    LOGGING_FN_ID = "LoggingFn"
    LOGGING_CODE_DIR = "lambdas/logging"
    BUCKET_CONSTRUCT_ID = "TestBucket"
    TOPIC_ID = "S3EventsTopic"
    QUEUE_ID = "SizeTrackerQueue"
    DLQ_ID = "SizeTrackerDLQ"
    LOGGING_QUEUE_ID = "LoggingQueue"
    LOGGING_DLQ_ID = "LoggingDLQ"
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

        # Shared envelope-parsing helper (s3_events.py) attached to every
        # SQS-triggered consumer so we don't duplicate the two-JSON-loads peel.
        s3_events_layer = lambda_.LayerVersion(
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
            layers=[s3_events_layer],
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

        # Second consumer on the same topic: a logger that writes one JSON
        # line per S3 event. Independent queue + DLQ so a stall here doesn't
        # back up the size tracker (and vice versa) -- that's the whole point
        # of the fanout pattern.
        self.logging_dlq = sqs.Queue(
            self,
            self.LOGGING_DLQ_ID,
            retention_period=Duration.days(14),
        )
        self.logging_queue = sqs.Queue(
            self,
            self.LOGGING_QUEUE_ID,
            # Logger sleeps ~2s per delete before its FilterLogEvents call
            # (see LOOKUP_DELAY_SECONDS in the handler), so keep the visibility
            # timeout comfortably above fn timeout for the same reason as above.
            visibility_timeout=Duration.seconds(60),
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=self.logging_dlq,
                max_receive_count=5,
            ),
        )
        self.topic.add_subscription(sns_subs.SqsSubscription(self.logging_queue))

        # Create the log group up-front so we can (a) pass its NAME to the fn
        # via env var (the handler needs it for filter_log_events) and (b)
        # scope the FilterLogEvents grant to the exact log group ARN.
        logging_log_group = logs.LogGroup(
            self,
            f"{self.LOGGING_FN_ID}Logs",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        logging_fn = lambda_.Function(
            self,
            self.LOGGING_FN_ID,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(self.LOGGING_CODE_DIR),
            layers=[s3_events_layer],
            timeout=Duration.seconds(30),
            log_group=logging_log_group,
            environment={
                "LOG_GROUP_NAME": logging_log_group.log_group_name,
            },
        )
        # Delete-size lookup reads the fn's own log group via filter_log_events;
        # least-privilege grant scoped to just that group's ARN.
        logging_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:FilterLogEvents"],
                resources=[logging_log_group.log_group_arn],
            )
        )
        logging_fn.add_event_source(
            lambda_events.SqsEventSource(
                self.logging_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        CfnOutput(self, "BucketName", value=self.bucket.bucket_name)
        CfnOutput(self, "TopicArn", value=self.topic.topic_arn)
        CfnOutput(self, "QueueUrl", value=self.queue.queue_url)
        CfnOutput(self, "DLQUrl", value=self.dlq.queue_url)
        CfnOutput(self, "LoggingQueueUrl", value=self.logging_queue.queue_url)
        CfnOutput(self, "LoggingDLQUrl", value=self.logging_dlq.queue_url)
        CfnOutput(self, "LoggingLogGroupName", value=logging_log_group.log_group_name)
