"""Delete the largest object in the bucket when the size alarm fires.

Triggered by a CloudWatch alarm's LambdaAction. We don't parse the alarm event
to figure out which bucket to clean -- there is exactly one bucket per stack,
so the name is passed in via BUCKET_NAME env var. That also side-steps the
awkward Trigger-with-Metrics event shape that CloudWatch uses when the alarm
is on a metric-math expression (our alarm converts bytes->KB via math).

Per assignment spec: delete only one object per invocation. The alarm's action
only fires on OK->ALARM state transitions, but the caller has decided that a
single delete will always drop the bucket below threshold, so we don't loop.
"""

import os

import boto3

S3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]


def _largest_object(bucket_name):
    paginator = S3.get_paginator("list_objects_v2")
    largest = None
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            if largest is None or obj["Size"] > largest["Size"]:
                largest = obj
    return largest


def lambda_handler(event, context):
    largest = _largest_object(BUCKET_NAME)
    if largest is None:
        # Shouldn't happen -- the alarm can't fire on an empty bucket -- but
        # guard so we don't crash on the edge case where a concurrent delete
        # raced us.
        print(f"{BUCKET_NAME}: empty, nothing to delete")
        return {"deleted": None}

    S3.delete_object(Bucket=BUCKET_NAME, Key=largest["Key"])
    print(f"{BUCKET_NAME}: deleted {largest['Key']} (size={largest['Size']} bytes)")
    return {"deleted": {"key": largest["Key"], "size": largest["Size"]}}
