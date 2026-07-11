from aws_cdk.assertions import Match, Template


def test_auto_cleanup_logger_wired_to_topic_with_dlq(stacks):
    template = Template.from_stack(stacks["AutoCleanupStack"])

    # Logger fn present (identified by LOG_GROUP_NAME env var).
    template.has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like(
            {
                "Handler": "lambda_function.lambda_handler",
                "Environment": {
                    "Variables": Match.object_like(
                        {"LOG_GROUP_NAME": Match.any_value()}
                    )
                },
            }
        ),
    )

    # Subscribed to the ingest topic via SQS (topic itself lives in
    # IngestStack, so we only see the subscription resource here).
    template.resource_count_is("AWS::SNS::Subscription", 1)
    template.has_resource_properties(
        "AWS::SNS::Subscription", Match.object_like({"Protocol": "sqs"})
    )

    # Logger's queue + DLQ.
    template.resource_count_is("AWS::SQS::Queue", 2)
    template.has_resource_properties(
        "AWS::SQS::Queue",
        Match.object_like(
            {"RedrivePolicy": Match.object_like({"maxReceiveCount": 5})}
        ),
    )
    template.has_resource_properties(
        "AWS::Lambda::EventSourceMapping",
        Match.object_like({"FunctionResponseTypes": ["ReportBatchItemFailures"]}),
    )

    # Logger fn has FilterLogEvents on its own log group.
    template.has_resource_properties(
        "AWS::IAM::Policy",
        Match.object_like(
            {
                "PolicyDocument": Match.object_like(
                    {
                        "Statement": Match.array_with(
                            [Match.object_like({"Action": "logs:FilterLogEvents"})]
                        )
                    }
                )
            }
        ),
    )


def test_auto_cleanup_metric_filter_alarm_and_cleaner(stacks):
    template = Template.from_stack(stacks["AutoCleanupStack"])

    # Metric filter emits TotalObjectSize under Assignment4App with BucketName
    # pulled from the logged JSON so we get one time series per bucket. The
    # pattern also excludes plot.png so the plot lambda's output doesn't
    # contribute to the alarm's SUM.
    template.has_resource_properties(
        "AWS::Logs::MetricFilter",
        Match.object_like(
            {
                "FilterPattern": Match.string_like_regexp(
                    r'\$\.size_delta.*\$\.object_name != "plot"'
                ),
                "MetricTransformations": [
                    Match.object_like(
                        {
                            "MetricNamespace": "Assignment4App",
                            "MetricName": "TotalObjectSize",
                            "MetricValue": "$.size_delta",
                            "Dimensions": [
                                {"Key": "BucketName", "Value": "$.bucket_name"}
                            ],
                        }
                    )
                ],
            }
        ),
    )

    # Cleaner lambda exists (identified by BUCKET_NAME env var).
    template.has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like(
            {
                "Handler": "lambda_function.lambda_handler",
                "Environment": {
                    "Variables": Match.object_like(
                        {"BUCKET_NAME": Match.any_value()}
                    )
                },
            }
        ),
    )

    # Cleaner must have S3 DeleteObject on the bucket. grant_delete emits a
    # separate policy statement whose Action is a single string, not an array,
    # so match that shape directly.
    template.has_resource_properties(
        "AWS::IAM::Policy",
        Match.object_like(
            {
                "PolicyDocument": Match.object_like(
                    {
                        "Statement": Match.array_with(
                            [Match.object_like({"Action": "s3:DeleteObject*"})]
                        )
                    }
                )
            }
        ),
    )

    # Single alarm: threshold=20 (bytes), fires on OK -> ALARM at > threshold,
    # SUM statistic on the TotalObjectSize metric with the BucketName
    # dimension. Metric is emitted in bytes so no math expression is needed.
    template.resource_count_is("AWS::CloudWatch::Alarm", 1)
    template.has_resource_properties(
        "AWS::CloudWatch::Alarm",
        Match.object_like(
            {
                "Threshold": 20,
                "ComparisonOperator": "GreaterThanThreshold",
                "EvaluationPeriods": 1,
                "TreatMissingData": "notBreaching",
                "Namespace": "Assignment4App",
                "MetricName": "TotalObjectSize",
                "Statistic": "Sum",
                "Dimensions": [Match.object_like({"Name": "BucketName"})],
                # CDK only emits AlarmActions into the template when at least
                # one action has been added, so asserting the key is present
                # transitively proves an action is wired. The concrete wiring
                # is verified by the Lambda permission assertion below.
                "AlarmActions": Match.any_value(),
            }
        ),
    )

    # Cleaner grants CloudWatch permission to invoke it.
    template.has_resource_properties(
        "AWS::Lambda::Permission",
        Match.object_like(
            {
                "Action": "lambda:InvokeFunction",
                "Principal": "lambda.alarms.cloudwatch.amazonaws.com",
            }
        ),
    )
