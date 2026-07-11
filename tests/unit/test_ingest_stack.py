from aws_cdk.assertions import Match, Template


def test_ingest_stack_wires_s3_to_sns_to_size_tracker(stacks):
    template = Template.from_stack(stacks["IngestStack"])

    # Size-tracker function (identified by its GSI_PARTITION_VALUE env var).
    template.has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like(
            {
                "Handler": "lambda_function.lambda_handler",
                "Environment": {
                    "Variables": Match.object_like(
                        {"GSI_PARTITION_VALUE": "ALL_BUCKETS"}
                    )
                },
            }
        ),
    )

    # S3 -> SNS notification (fanout entry point).
    template.resource_count_is("Custom::S3BucketNotifications", 1)
    template.resource_count_is("AWS::SNS::Topic", 1)

    # One subscription lives in THIS stack (size tracker); the logger's
    # subscription is added later from AutoCleanupStack against the same
    # topic and shows up in that stack's template, not this one.
    template.resource_count_is("AWS::SNS::Subscription", 1)
    template.has_resource_properties(
        "AWS::SNS::Subscription", Match.object_like({"Protocol": "sqs"})
    )

    # Size tracker's queue + DLQ, and the redrive policy.
    template.resource_count_is("AWS::SQS::Queue", 2)
    template.has_resource_properties(
        "AWS::SQS::Queue",
        Match.object_like(
            {"RedrivePolicy": Match.object_like({"maxReceiveCount": 5})}
        ),
    )

    # Size tracker uses partial-batch failure reporting.
    template.has_resource_properties(
        "AWS::Lambda::EventSourceMapping",
        Match.object_like({"FunctionResponseTypes": ["ReportBatchItemFailures"]}),
    )
