from aws_cdk.assertions import Match, Template


def test_driver_stack_injects_api_url(stacks):
    template = Template.from_stack(stacks["DriverStack"])
    template.has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like(
            {
                "Handler": "lambda_function.lambda_handler",
                "Timeout": 60,
                "Environment": {
                    "Variables": Match.object_like(
                        {
                            "PLOT_API_URL": Match.any_value(),
                            "BUCKET_NAME": Match.any_value(),
                        }
                    )
                },
            }
        ),
    )
