"""Envelope parsing for S3 -> SNS -> SQS fanout.

Shared by every SQS-triggered consumer of the S3 events topic: peels the two
JSON envelopes (SQS `body` -> SNS notification -> S3 event) and yields the
underlying S3 records.
"""

import json


def s3_records_from_sqs_record(sqs_record):
    """Yield each S3 event record wrapped inside one SQS message."""
    sns_envelope = json.loads(sqs_record["body"])
    s3_event = json.loads(sns_envelope["Message"])
    yield from s3_event.get("Records", [])
