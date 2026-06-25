"""
Create DynamoDB table + Global secondary index
"""

import aws_cdk.aws_dynamodb as dynamodb
from aws_cdk import CfnOutput, RemovalPolicy, Stack
from constructs import Construct


class DataStack(Stack):
    # The lambdas receive these as env vars.
    TABLE_CONSTRUCT_ID = "SizeHistoryTable"
    GSI_NAME = "BucketSizeIndex"
    GSI_PARTITION_VALUE = "ALL_BUCKETS"

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # CloudFormation dynamically generates a unique name DynamoDB table.
        self.table = dynamodb.Table(
            self,
            self.TABLE_CONSTRUCT_ID,
            partition_key=dynamodb.Attribute(
                name="BucketName", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="Timestamp", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            # DESTROY so `cdk destroy` fully cleans up, like `main.py teardown`.
            removal_policy=RemovalPolicy.DESTROY,
        )

        # IndexPK / BucketSize attribute definitions are inferred from the key
        # schema below -- no need to declare them separately as main.py did.
        self.table.add_global_secondary_index(
            index_name=self.GSI_NAME,
            partition_key=dynamodb.Attribute(
                name="IndexPK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="BucketSize", type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        CfnOutput(self, "TableName", value=self.table.table_name)
        CfnOutput(self, "GsiName", value=self.GSI_NAME)
