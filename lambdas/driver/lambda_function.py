"""Drive the size-tracking + auto-cleanup pipeline end-to-end.

No more manual deletes -- the Cleaner lambda takes care of those when the
CloudWatch alarm fires. This driver just creates objects on a schedule that
gives the alarm-eval-then-Cleaner cycle time to run between steps, then calls
the plot API so the resulting graph shows the whole story.

The sleeps below are sized so each create lands in its own 1-minute alarm
evaluation period; that maximizes the chance of a clean OK -> ALARM -> OK ->
ALARM sequence. See auto_cleanup_stack.py for the SUM-period caveat -- with
delta-based metrics + a 20-byte threshold, some firings can still be missed
depending on where events land relative to wall-clock minute boundaries.
"""

import os
import time

import boto3
import urllib3

S3 = boto3.client("s3")

TARGET_BUCKET = os.environ["BUCKET_NAME"]
PLOT_API_URL = os.environ["PLOT_API_URL"]

http = urllib3.PoolManager()

# 30s past one alarm period (60s). Long enough that the alarm has time to
# evaluate + fire + Cleaner to run + the delete-delta to propagate through
# the metric filter before the next PUT, with headroom against timing
# jitter. Also spaces the driver's PUTs far enough apart that the plot's
# datapoints don't visually pile up. Going lower (e.g. 65s) races the
# Cleaner and drifts datapoints across wall-clock period boundaries.
STEP_DELAY_SECONDS = 90


def _put(key, body):
    S3.put_object(Bucket=TARGET_BUCKET, Key=key, Body=body)
    print(f"PUT {key} ({len(body)} bytes)")


def lambda_handler(event, context):
    # 1. Create assignment1.txt (18 bytes). Bucket size = 18, below threshold.
    _put("assignment1.txt", "Empty Assignment 1")
    time.sleep(STEP_DELAY_SECONDS)

    # 2. Create assignment2.txt (28 bytes). Combined size = 46, above 20-byte
    #    threshold -> alarm fires -> Cleaner deletes the LARGEST object, which
    #    is assignment2.txt.
    _put("assignment2.txt", "Empty Assignment 2222222222")
    time.sleep(STEP_DELAY_SECONDS)

    # 3. Create assignment3.txt (2 bytes). Whether the alarm fires again here
    #    depends on how the datapoints line up in the current SUM window --
    #    the Cleaner's -28 delete-delta and this +2 create-delta may or may
    #    not land in the same 1-minute period as any lingering positive
    #    datapoints. See the auto_cleanup_stack docstring for details.
    _put("assignment3.txt", "33")
    time.sleep(STEP_DELAY_SECONDS)

    # 4. Trigger the plot lambda so the graph reflects the whole sequence
    #    (creates + Cleaner's deletes) from DynamoDB.
    url = f"{PLOT_API_URL}?bucket={TARGET_BUCKET}"
    print("CALLING:", url)
    response = http.request("GET", url)

    return {
        "statusCode": response.status,
        "body": response.data.decode(),
    }
