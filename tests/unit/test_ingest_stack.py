from aws_cdk.assertions import Match, Template


def test_ingest_stack_triggers_size_lambda_from_bucket(stacks):
    template = Template.from_stack(stacks["IngestStack"])
    # the size-tracking function (identified by its literal env var)
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
    # S3 -> Lambda notification wiring exists
    template.resource_count_is("Custom::S3BucketNotifications", 1)
