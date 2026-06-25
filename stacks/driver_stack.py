"""
Create Driver lambda
"""

import aws_cdk.aws_apigateway as apigateway
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from constructs import Construct
from stacks.plot_api_stack import PlotApiStack


class DriverStack(Stack):
    DRIVER_FN_ID = "DriverFn"
    DRIVER_CODE_DIR = "lambdas/driver"

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket: s3.IBucket,
        plot_api: apigateway.RestApi,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        driver_fn = lambda_.Function(
            self,
            self.DRIVER_FN_ID,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(self.DRIVER_CODE_DIR),
            timeout=Duration.minutes(1),  # the manual "increase timeout to 1 min"
            log_group=logs.LogGroup(
                self,
                f"{self.DRIVER_FN_ID}Logs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            ),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "PLOT_API_URL": plot_api.url_for_path(f"/{PlotApiStack.REST_API_PATH}"),
            },
        )

        # The driver uploads/deletes test objects, then calls the public API
        # over HTTP (the GET method has no IAM auth, so no invoke grant needed).
        bucket.grant_read_write(driver_fn)

        CfnOutput(self, "DriverFunctionName", value=driver_fn.function_name)
