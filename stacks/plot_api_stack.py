"""
Create Plotting lambda with matplotlib layer + REST API for the plotting lambda
"""

import aws_cdk.aws_apigateway as apigateway
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_lambda_python_alpha as lambda_python
from constructs import Construct


class PlotApiStack(Stack):
    PLOTTING_FN_ID = "PlottingFn"
    PLOTTING_CODE_DIR = "lambdas/plotter"
    REST_API_ID = "BucketSizeHistoryPlot"
    REST_API_PATH = "plot"
    REST_API_METHOD = "GET"
    REST_API_DEPLOY_STAGE = "dev"
    # Object key the plotter uses when writing the generated PNG into the
    # bucket. Exposed as a class attribute so AutoCleanupStack can exclude it
    # from the size alarm's metric filter without duplicating the literal.
    PLOT_OBJECT_KEY = "plot"

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        table: dynamodb.ITable,
        gsi_name: str,
        gsi_partition_value: str,
        bucket: s3.IBucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Build the matplotlib layer
        mpl_layer = lambda_python.PythonLayerVersion(
            self,
            "MatplotlibLayer",
            entry="layers/matplotlib",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
        )

        plot_fn = lambda_.Function(
            self,
            self.PLOTTING_FN_ID,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(self.PLOTTING_CODE_DIR),
            layers=[mpl_layer],
            timeout=Duration.seconds(30),  # the manual "increase timeout to 30s"
            memory_size=512,  # matplotlib is memory-hungry
            log_group=logs.LogGroup(
                self,
                f"{self.PLOTTING_FN_ID}Logs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            ),
            environment={
                "TABLE_NAME": table.table_name,
                "GSI_NAME": gsi_name,
                "GSI_PARTITION_VALUE": gsi_partition_value,
                "BUCKET_NAME": bucket.bucket_name,  # where the plot is written
                # Pin the plotter's output key to the stack's constant so the
                # AutoCleanupStack exclusion can't drift out of sync.
                "PLOT_KEY": self.PLOT_OBJECT_KEY,
            },
        )

        # Least-privilege replacement for the manual customer-managed policy.
        table.grant_read_data(plot_fn)  # Query on the table AND its indexes
        bucket.grant_put(plot_fn)  # PutObject for plot.png

        # REST API; no rest_api_name -> auto-generated. Deployed to `dev`.
        self.api = apigateway.RestApi(
            self,
            self.REST_API_ID,
            deploy_options=apigateway.StageOptions(
                stage_name=self.REST_API_DEPLOY_STAGE
            ),
        )
        plot = self.api.root.add_resource(self.REST_API_PATH)  # the /plot path
        # proxy=True == "Lambda proxy integration"; also grants invoke perms.
        plot.add_method(
            self.REST_API_METHOD, apigateway.LambdaIntegration(plot_fn, proxy=True)
        )

        CfnOutput(
            self, "PlotApiUrl", value=self.api.url_for_path(f"/{self.REST_API_PATH}")
        )
