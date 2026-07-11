"""
Self-healing feedback loop on top of the ingest topic:

    SNS S3EventsTopic
        |
        v
    SQS LoggingQueue -> LoggingFn -> log group
      (DLQ after 5)                    |
                                       v
                      MetricFilter -> Assignment4App/TotalObjectSize
                                       |
                                       v
                       Alarm (SUM > 20 KB) --> Cleaner --> S3 delete
                                                            (feeds back
                                                             into IngestStack's
                                                             topic)

Split out of IngestStack so the "event source" (bucket + topic + size tracker)
and the "control loop" (logger + metric filter + alarm + cleaner) can evolve
independently. Cross-stack refs: bucket for Cleaner's grants, topic for the
logger's subscription, shared s3_events layer for envelope parsing.
"""

import aws_cdk.aws_cloudwatch as cloudwatch
import aws_cdk.aws_cloudwatch_actions as cw_actions
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_lambda_event_sources as lambda_events
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subscriptions as sns_subs
import aws_cdk.aws_sqs as sqs
from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from constructs import Construct
from stacks.plot_api_stack import PlotApiStack


class AutoCleanupStack(Stack):
    LOGGING_FN_ID = "LoggingFn"
    LOGGING_CODE_DIR = "lambdas/logging"
    LOGGING_QUEUE_ID = "LoggingQueue"
    LOGGING_DLQ_ID = "LoggingDLQ"
    CLEANER_FN_ID = "Cleaner"
    CLEANER_CODE_DIR = "lambdas/cleaner"
    METRIC_NAMESPACE = "Assignment4App"
    METRIC_NAME = "TotalObjectSize"
    ALARM_ID = "CleanerAlarm"
    # Threshold the assignment fixes: SUM of size_delta at or above 20 bytes
    # fires the alarm. Comparison is >= (not >) so a bucket that lands exactly
    # at 20 still triggers cleanup -- the intent is "keep size strictly below
    # 20 bytes", not "at most 20". The metric is already in bytes (size_delta
    # comes from the S3 event as bytes), so no unit conversion is needed.
    ALARM_THRESHOLD_BYTES = 20

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket: s3.IBucket,
        topic: sns.ITopic,
        s3_events_layer: lambda_.ILayerVersion,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Consumer of the ingest topic. Independent queue + DLQ so a stall in
        # the logger doesn't back up the size tracker (and vice versa) --
        # that's the whole point of the fanout pattern.
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
            # timeout comfortably above fn timeout.
            visibility_timeout=Duration.seconds(60),
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=self.logging_dlq,
                max_receive_count=5,
            ),
        )
        topic.add_subscription(sns_subs.SqsSubscription(self.logging_queue))

        # Log group created up-front so we can (a) pass its NAME to the fn via
        # env var (the handler needs it for filter_log_events) and (b) scope
        # both the FilterLogEvents grant and the metric filter to it.
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

        # Metric filter turns every "{...size_delta: N...}" log line into a
        # CloudWatch metric datapoint. Dimension is pulled from the JSON so we
        # get one time series per bucket automatically; missing/null size_delta
        # entries (the miss-warning path) don't publish because the JSON
        # pattern requires size_delta to exist AND be numeric.
        #
        # Exclude the plot lambda's output object from the alarm's SUM --
        # that's app-produced output written to the same bucket, and it can
        # easily exceed the byte threshold on its own. Excluding it lets the
        # plot survive long enough to be downloaded instead of getting cleaned
        # as soon as the plot lambda finishes. The exact key is owned by
        # PlotApiStack so both sides can't drift.
        logs.MetricFilter(
            self,
            "TotalObjectSizeFilter",
            log_group=logging_log_group,
            metric_namespace=self.METRIC_NAMESPACE,
            metric_name=self.METRIC_NAME,
            filter_pattern=logs.FilterPattern.all(
                logs.FilterPattern.exists("$.size_delta"),
                logs.FilterPattern.string_value(
                    "$.object_name", "!=", PlotApiStack.PLOT_OBJECT_KEY
                ),
            ),
            metric_value="$.size_delta",
            dimensions={"BucketName": "$.bucket_name"},
        )

        # Control-loop consumer: fired by the alarm below, deletes the largest
        # object in the bucket. Not an SNS subscriber -- alarm actions invoke
        # Lambda directly, no queue in between.
        cleaner_fn = lambda_.Function(
            self,
            self.CLEANER_FN_ID,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(self.CLEANER_CODE_DIR),
            timeout=Duration.seconds(30),
            log_group=logs.LogGroup(
                self,
                f"{self.CLEANER_FN_ID}Logs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            ),
            environment={
                # Only one bucket per app, so pass the name directly instead
                # of parsing it out of the alarm event (which is awkwardly
                # shaped when the alarm is on a metric math expression).
                "BUCKET_NAME": bucket.bucket_name,
            },
        )
        # Cleaner needs to list the bucket to find the biggest object, then
        # delete it. grant_read gives ListBucket + GetObject; grant_delete adds
        # DeleteObject. GetObject isn't strictly needed but comes with read().
        bucket.grant_read(cleaner_fn)
        bucket.grant_delete(cleaner_fn)

        # Metric matches the assignment's log format: `{"size_delta": 98}` for
        # 98 bytes. Alarm compares SUM directly against the byte threshold.
        #
        # SUM-period caveat (documented in the assignment): a 1-minute SUM
        # aggregates whatever datapoints happen to land in the same wall-clock
        # minute, so two rapid PUTs straddling a minute boundary won't add up
        # and might miss the threshold. Accepted tradeoff -- shorter period
        # keeps the "complete period" delay small, and the assignment
        # explicitly says spurious/missed firings are OK to reason about.
        # treat_missing_data=NOT_BREACHING so quiet stretches don't flip the
        # alarm into INSUFFICIENT_DATA and then back to OK.
        size_metric = cloudwatch.Metric(
            namespace=self.METRIC_NAMESPACE,
            metric_name=self.METRIC_NAME,
            dimensions_map={"BucketName": bucket.bucket_name},
            statistic="Sum",
            period=Duration.minutes(1),
        )
        self.alarm = cloudwatch.Alarm(
            self,
            self.ALARM_ID,
            metric=size_metric,
            threshold=self.ALARM_THRESHOLD_BYTES,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        # Alarm action fires ONCE on OK -> ALARM transition (not on every
        # datapoint while in ALARM). Assignment guarantees a single delete
        # drops us below threshold, so we don't need a fan-out or retry.
        self.alarm.add_alarm_action(cw_actions.LambdaAction(cleaner_fn))

        CfnOutput(self, "LoggingQueueUrl", value=self.logging_queue.queue_url)
        CfnOutput(self, "LoggingDLQUrl", value=self.logging_dlq.queue_url)
        CfnOutput(self, "LoggingLogGroupName", value=logging_log_group.log_group_name)
        CfnOutput(self, "CleanerAlarmName", value=self.alarm.alarm_name)
