from aws_cdk.assertions import Match, Template


def test_ingest_stack_fans_out_s3_events_to_two_consumers(stacks):
    template = Template.from_stack(stacks["IngestStack"])

    # Both consumer functions exist (identified by their unique env vars).
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

    # S3 -> SNS notification (single hub feeding both consumers).
    template.resource_count_is("Custom::S3BucketNotifications", 1)
    template.resource_count_is("AWS::SNS::Topic", 1)

    # One SNS subscription per consumer.
    template.resource_count_is("AWS::SNS::Subscription", 2)
    template.all_resources_properties(
        "AWS::SNS::Subscription", Match.object_like({"Protocol": "sqs"})
    )

    # Four queues total: two main + two DLQs. Both main queues must have a
    # redrive policy pointing at their DLQ.
    template.resource_count_is("AWS::SQS::Queue", 4)
    template.resource_properties_count_is(
        "AWS::SQS::Queue",
        Match.object_like({"RedrivePolicy": Match.object_like({"maxReceiveCount": 5})}),
        2,
    )

    # Both consumers use SQS event sources with partial-batch failure reporting.
    template.resource_properties_count_is(
        "AWS::Lambda::EventSourceMapping",
        Match.object_like({"FunctionResponseTypes": ["ReportBatchItemFailures"]}),
        2,
    )

    # Logging fn has FilterLogEvents on its own log group.
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
