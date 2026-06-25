from aws_cdk.assertions import Match, Template


def test_data_stack_creates_table_with_gsi(stacks):
    template = Template.from_stack(stacks["DataStack"])
    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": Match.array_with(
                [Match.object_like({"IndexName": "BucketSizeIndex"})]
            ),
        },
    )
