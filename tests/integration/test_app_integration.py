import pytest
from aws_cdk import App
from aws_cdk.assertions import Match, Template

from stacks.data_stack import DataStack
from stacks.ingest_stack import IngestStack
from stacks.driver_stack import DriverStack


def test_full_app_wires_stacks_together(stacks):
    """Happy path: the whole graph synthesizes and cross-stack values resolve --
    the plotting lambda reads DataStack's table, the driver reads PlotApiStack's
    API URL."""
    plot_template = Template.from_stack(stacks["PlotApiStack"])
    driver_template = Template.from_stack(stacks["DriverStack"])

    plot_template.has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like(
            {"Environment": {"Variables": Match.object_like({"TABLE_NAME": Match.any_value()})}}
        ),
    )
    driver_template.has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like(
            {"Environment": {"Variables": Match.object_like({"PLOT_API_URL": Match.any_value()})}}
        ),
    )


def test_driver_stack_requires_plot_api():
    """Error path: plot_api is a required dependency; omitting it fails fast."""
    app = App(context={"aws:cdk:bundling-stacks": []})
    data = DataStack(app, "DataStack")
    ingest = IngestStack(
        app,
        "IngestStack",
        table=data.table,
        gsi_partition_value=DataStack.GSI_PARTITION_VALUE,
    )
    with pytest.raises(TypeError):
        DriverStack(app, "DriverStack", bucket=ingest.bucket)  # missing plot_api
