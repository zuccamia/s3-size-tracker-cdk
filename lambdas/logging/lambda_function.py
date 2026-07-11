"""Log a JSON line per S3 event: {"object_name": ..., "size_delta": ...}.

Creates use the size straight from the event. Deletes don't carry a size, so
we grep our own log group for the most recent create of the same key and
negate its size_delta. If we can't find one (log retention aged the create out,
or the object never had a create logged), we emit size_delta=null plus a
"warning" field so a downstream reader can spot the gap.

Cost/latency note for future consideration: every delete costs one
`filter_log_events` API call plus a fixed sleep (see LOOKUP_DELAY_SECONDS).
That's fine at assignment scale, but at high delete throughput it would
dominate this lambda's cost and wall-clock time. If we ever hit that regime,
the cleaner design is a small DynamoDB "latest-known-size per (bucket, key)"
table maintained on every create, which turns the delete lookup into an O(1)
GetItem instead of a log-scan API call.
"""

import json
import os
import time

import boto3
from s3_events import s3_records_from_sqs_record

LOGS = boto3.client("logs")

LOG_GROUP_NAME = os.environ["LOG_GROUP_NAME"]

# Short pause before the create-log lookup so the earlier "create" log has time
# to become searchable via filter_log_events (CloudWatch Logs is eventually
# consistent). Two seconds is empirically enough for same-invocation writes,
# and cheap enough at assignment scale.
LOOKUP_DELAY_SECONDS = 2

# How far back to search for the matching create log. Bounded so a delete of a
# very old object doesn't scan the whole retention window every time.
LOOKUP_WINDOW_MS = 7 * 24 * 60 * 60 * 1000  # 7 days -- matches log retention


def _emit(payload):
    # The delete-side lookup uses filter_log_events with a JSON pattern, so
    # every line we emit MUST be a single-line JSON object. print() adds \n.
    print(json.dumps(payload))


def _lookup_create_size(object_name):
    # Most recent creation for this key. filter_log_events returns oldest
    # first, so `events[-1]` is the latest match within the window.
    start_time = int(time.time() * 1000) - LOOKUP_WINDOW_MS
    resp = LOGS.filter_log_events(
        logGroupName=LOG_GROUP_NAME,
        startTime=start_time,
        filterPattern=f'{{ $.object_name = "{object_name}" && $.size_delta > 0 }}',
    )
    events = resp.get("events", [])
    if not events:
        return None
    try:
        return json.loads(events[-1]["message"])["size_delta"]
    except (KeyError, ValueError):
        return None


def _handle_s3_record(record):
    event_name = record.get("eventName", "")
    object_name = record["s3"]["object"]["key"]

    if event_name.startswith("ObjectCreated:"):
        size = record["s3"]["object"]["size"]
        _emit({"object_name": object_name, "size_delta": size})
        return

    if event_name.startswith("ObjectRemoved:"):
        time.sleep(LOOKUP_DELAY_SECONDS)
        prior_size = _lookup_create_size(object_name)
        if prior_size is None:
            _emit(
                {
                    "object_name": object_name,
                    "size_delta": None,
                    "warning": "no prior create log found within lookup window",
                }
            )
        else:
            _emit({"object_name": object_name, "size_delta": -prior_size})
        return

    # Unknown event class -- record it but don't fail the batch.
    print(f"ignored eventName={event_name!r} for key={object_name!r}")


def lambda_handler(event, context):
    batch_item_failures = []
    for sqs_record in event.get("Records", []):
        try:
            for s3_record in s3_records_from_sqs_record(sqs_record):
                _handle_s3_record(s3_record)
        except Exception as exc:
            print(f"failed message {sqs_record.get('messageId')}: {exc}")
            batch_item_failures.append(
                {"itemIdentifier": sqs_record["messageId"]}
            )
    return {"batchItemFailures": batch_item_failures}
