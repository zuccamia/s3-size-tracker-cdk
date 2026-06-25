import os
import time

import boto3
import urllib3

S3 = boto3.client("s3")

TARGET_BUCKET = os.environ["BUCKET_NAME"]
PLOT_API_URL = os.environ["PLOT_API_URL"]

http = urllib3.PoolManager()


def lambda_handler(event, context):
    # 1. Create assignment1.txt (19 bytes)
    S3.put_object(
        Bucket=TARGET_BUCKET,
        Key="assignment1.txt",
        Body="Empty Assignment 1",
    )
    time.sleep(2)

    # 2. Update assignment1.txt (28 bytes)
    S3.put_object(
        Bucket=TARGET_BUCKET,
        Key="assignment1.txt",
        Body="Empty Assignment 2222222222",
    )
    time.sleep(2)

    # 3. Delete assignment1.txt (0 bytes)
    S3.delete_object(
        Bucket=TARGET_BUCKET,
        Key="assignment1.txt",
    )
    time.sleep(2)

    # 4. Create assignment2.txt (2 bytes)
    S3.put_object(
        Bucket=TARGET_BUCKET,
        Key="assignment2.txt",
        Body="33",
    )
    time.sleep(2)

    # 5. Call the plotting lambda
    url = f"{PLOT_API_URL}?bucket={TARGET_BUCKET}"
    print("CALLING:", url)
    response = http.request("GET", url)

    return {
        "statusCode": response.status,
        "body": response.data.decode(),
    }
