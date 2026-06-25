"""Shared fixture: build all four stacks in one App.

Docker bundling is disabled via `aws:cdk:bundling-stacks` so synthesizing the
matplotlib layer doesn't spin up a container during tests.
"""
import pytest
from aws_cdk import App

from stacks.data_stack import DataStack
from stacks.ingest_stack import IngestStack
from stacks.plot_api_stack import PlotApiStack
from stacks.driver_stack import DriverStack


@pytest.fixture
def stacks():
    app = App(context={"aws:cdk:bundling-stacks": []})
    data = DataStack(app, "DataStack")
    ingest = IngestStack(
        app,
        "IngestStack",
        table=data.table,
        gsi_partition_value=DataStack.GSI_PARTITION_VALUE,
    )
    plot_api = PlotApiStack(
        app,
        "PlotApiStack",
        table=data.table,
        gsi_name=DataStack.GSI_NAME,
        gsi_partition_value=DataStack.GSI_PARTITION_VALUE,
        bucket=ingest.bucket,
    )
    driver = DriverStack(
        app,
        "DriverStack",
        bucket=ingest.bucket,
        plot_api=plot_api.api,
    )
    return {
        "DataStack": data,
        "IngestStack": ingest,
        "PlotApiStack": plot_api,
        "DriverStack": driver,
    }
