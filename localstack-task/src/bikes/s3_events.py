"""S3 event notifications on the queue: reading them, and making one"""

from __future__ import annotations

import json
from urllib.parse import quote_plus, unquote_plus

from bikes.resources import BUCKET

TEST_EVENT = "s3:TestEvent"


def object_keys(body: str) -> list[str] | None:
    """The object keys an SQS message body reports, or None for S3's test event"""
    event = json.loads(body)
    if event.get("Event") == TEST_EVENT:
        return None
    return [unquote_plus(record["s3"]["object"]["key"]) for record in event.get("Records", [])]


def event_body(key: str, bucket: str = BUCKET) -> str:
    """The message S3 sends for a new object at key; `reload` queues these"""
    record = {
        "eventSource": "aws:s3",
        "eventName": "ObjectCreated:Put",
        "s3": {"bucket": {"name": bucket}, "object": {"key": quote_plus(key, safe="/")}},
    }
    return json.dumps({"Records": [record]})
