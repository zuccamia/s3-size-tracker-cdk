import os
import time

import boto3

# S3 and DynamoDB live in the same region as this lambda, so boto3 picks up the
# region from the execution environment automatically -- no region_name needed.
S3 = boto3.client("s3")
DYNAMODB = boto3.client("dynamodb")

TABLE_NAME = os.environ["TABLE_NAME"]

# Must match the GSI partition value used everywhere else in the project. Every
# item carries it on IndexPK so (a) the row lands in BucketSizeIndex and (b) the
# plotting lambda can query the all-time max across ALL buckets in one shot,
# without a scan.
GSI_PARTITION_VALUE = os.environ.get("GSI_PARTITION_VALUE", "ALL_BUCKETS")


def extract_bucket_names(event):
    # One S3 notification can batch several records (and in theory span buckets),
    # so collect the distinct names. We recompute the whole bucket on each
    # trigger, so the individual changed keys don't matter -- only the bucket.
    names = set()
    for record in event.get("Records", []):
        names.add(record["s3"]["bucket"]["name"])
    return names


def compute_bucket_size(bucket_name):
    # Recompute from scratch: don't trust the event payload's per-object size,
    # re-list the bucket. Paginate so buckets with >1,000 objects are fully
    # counted. An empty bucket (e.g. right after the Part 4 delete) has no
    # 'Contents' key, which correctly yields size 0 / count 0.
    paginator = S3.get_paginator("list_objects_v2")
    total_size = 0
    object_count = 0
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            total_size += obj["Size"]
            object_count += 1
    return total_size, object_count


def record_size(bucket_name, total_size, object_count, timestamp):
    # DynamoDB Number attributes cross the low-level client API as strings.
    DYNAMODB.put_item(
        TableName=TABLE_NAME,
        Item={
            "BucketName": {"S": bucket_name},
            "Timestamp": {"N": str(timestamp)},
            "BucketSize": {"N": str(total_size)},
            "ObjectCount": {"N": str(object_count)},
            "IndexPK": {"S": GSI_PARTITION_VALUE},
        },
    )


def lambda_handler(event, context):
    recorded = []
    for bucket_name in extract_bucket_names(event):
        total_size, object_count = compute_bucket_size(bucket_name)
        # Epoch milliseconds: numeric, so the Part 3 "last 10 seconds" BETWEEN
        # query compares correctly, and fine-grained enough that two events
        # rarely collide on the (BucketName, Timestamp) sort key.
        timestamp = int(time.time() * 1000)
        record_size(bucket_name, total_size, object_count, timestamp)
        print(
            f"{bucket_name}: size={total_size} bytes, "
            f"objects={object_count}, ts={timestamp}"
        )
        recorded.append(
            {
                "bucket": bucket_name,
                "size": total_size,
                "count": object_count,
                "timestamp": timestamp,
            }
        )
    return {"recorded": recorded}
