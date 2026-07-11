import io
import os
import time

import boto3
import matplotlib

matplotlib.use("Agg")  # headless backend for Lambda
import matplotlib.pyplot as plt

S3 = boto3.client("s3")
DYNAMODB = boto3.client("dynamodb")

TABLE_NAME = os.environ["TABLE_NAME"]
GSI_NAME = os.environ["GSI_NAME"]
GSI_PARTITION_VALUE = os.environ.get("GSI_PARTITION_VALUE", "ALL_BUCKETS")

PLOT_OBJECT_KEY = os.environ.get("PLOT_KEY", "plot")
# Driver runs ~4.5 min (3 x 90s sleeps between PUTs, plus alarm/Cleaner cycle
# time), so anything shorter would miss most of the sequence. 10 minutes gives
# comfortable headroom for reruns and clock skew.
WINDOW_MS = 10 * 60 * 1000


def query_recent_sizes(bucket_name, now_ms):
    """PK = BucketName, SK = Timestamp BETWEEN (now-10s, now)."""
    resp = DYNAMODB.query(
        TableName=TABLE_NAME,
        KeyConditionExpression="BucketName = :bn AND #ts BETWEEN :lo AND :hi",
        ExpressionAttributeNames={"#ts": "Timestamp"},  # reserved word
        ExpressionAttributeValues={
            ":bn": {"S": bucket_name},
            ":lo": {"N": str(now_ms - WINDOW_MS)},
            ":hi": {"N": str(now_ms)},
        },
    )
    return resp["Items"]


def query_alltime_max():
    """Query the GSI descending by BucketSize, grab the first item."""
    resp = DYNAMODB.query(
        TableName=TABLE_NAME,
        IndexName=GSI_NAME,
        KeyConditionExpression="IndexPK = :pk",
        ExpressionAttributeValues={":pk": {"S": GSI_PARTITION_VALUE}},
        ScanIndexForward=False,  # descending → largest BucketSize first
        Limit=1,
    )
    items = resp["Items"]
    if not items:
        return 0
    return int(items[0]["BucketSize"]["N"])


def build_plot(items, bucket_name, max_size):
    """Line chart of recent sizes + horizontal line for the all-time max."""
    # Sort by timestamp so the line draws left-to-right.
    items.sort(key=lambda i: int(i["Timestamp"]["N"]))

    timestamps = [int(i["Timestamp"]["N"]) for i in items]
    sizes = [int(i["BucketSize"]["N"]) for i in items]

    # Convert epoch-ms to seconds-ago for a readable X axis.
    if timestamps:
        latest = timestamps[-1]
        x_vals = [(t - latest) / 1000 for t in timestamps]  # negative offsets
    else:
        x_vals = []

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(x_vals, sizes, marker="o", label=f"{bucket_name} size")
    ax.axhline(y=max_size, color="r", linestyle="--", label="All-time max")
    ax.set_xlabel("Seconds ago")
    ax.set_ylabel("Bucket size (bytes)")
    ax.set_title(f"{bucket_name} — last {WINDOW_MS // 60_000} minutes")
    ax.legend()
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf


def lambda_handler(event, context):
    print("EVENT:", event)
    params = event.get("queryStringParameters") or {}
    bucket_name = params.get("bucket")

    if not bucket_name:
        return {
            "statusCode": 400,
            "body": "Missing required query parameter: bucket",
        }

    now_ms = int(time.time() * 1000)

    items = query_recent_sizes(bucket_name, now_ms)
    max_size = query_alltime_max()

    png_buf = build_plot(items, bucket_name, max_size)

    S3.put_object(
        Bucket=bucket_name,
        Key=PLOT_OBJECT_KEY,
        Body=png_buf.read(),
        ContentType="image/png",
    )

    return {
        "statusCode": 200,
        "body": f"Plot saved to s3://{bucket_name}/{PLOT_OBJECT_KEY} "
        f"({len(items)} data points, all-time max={max_size})",
    }
