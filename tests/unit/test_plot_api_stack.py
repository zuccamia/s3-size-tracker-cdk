from aws_cdk.assertions import Match, Template


def test_plot_api_stack_exposes_get_plot(stacks):
    template = Template.from_stack(stacks["PlotApiStack"])
    template.resource_count_is("AWS::Lambda::LayerVersion", 1)  # matplotlib layer
    template.has_resource_properties(
        "AWS::ApiGateway::Method", Match.object_like({"HttpMethod": "GET"})
    )
    template.has_resource_properties(
        "AWS::ApiGateway::Stage", Match.object_like({"StageName": "dev"})
    )
