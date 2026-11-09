"""What the Lambda reads and writes: the month files, the load ledger, the tables, the queue"""

from __future__ import annotations

import math
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Any

from boto3.dynamodb.conditions import Attr, ConditionBase, Key
from botocore.exceptions import ClientError

from bikes.loader.items import Item
from bikes.loader.month import TableSlice
from bikes.resources import BUCKET, LOADS, TableSpec

LOADING, LOADED, FAILED = "loading", "loaded", "failed"
MAX_ERROR_LENGTH = 1_000

WRITE_THREADS = 8
ITEMS_PER_THREAD = 250


def utc_now() -> datetime:
    return datetime.now(UTC)


def stamp(moment: datetime) -> str:
    """A time as UTC ISO 8601 text, which sorts like the times do; the ledger compares these"""
    return moment.astimezone(UTC).isoformat(timespec="seconds")


def conditionally(call: Callable[..., Any], **kwargs: Any) -> bool:
    """Make a conditional write; False when DynamoDB found its condition false"""
    try:
        call(**kwargs)
    except ClientError as error:
        if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise
    return True


def all_items(call: Callable[..., Mapping[str, Any]], **kwargs: Any) -> Iterator[Item]:
    """Every item a Query or Scan returns, following LastEvaluatedKey page by page"""
    while True:
        page = call(**kwargs)
        yield from page.get("Items", [])
        if "LastEvaluatedKey" not in page:
            return
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


# --- S3


class S3Files:
    """The month files in S3: whether one is there, which version, and its text"""

    def __init__(self, s3: Any, bucket: str = BUCKET) -> None:
        self._s3 = s3
        self._bucket = bucket

    def etag(self, key: str) -> str | None:
        """The object's ETag, which changes with its content; None if it is missing"""
        listing = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=key, MaxKeys=1)
        found = (entry for entry in listing.get("Contents", []) if entry["Key"] == key)
        return next((entry["ETag"].strip('"') for entry in found), None)

    def read_text(self, key: str, etag: str) -> str:
        """The object's text if it is still the version etag names; else PreconditionFailed"""
        response = self._s3.get_object(Bucket=self._bucket, Key=key, IfMatch=f'"{etag}"')
        return response["Body"].read().decode("utf-8")


# --- bike_loads


@dataclass(frozen=True)
class MonthFiles:
    """A month's two files as S3 has them now, their ETags say which versions"""

    month: str
    raw_etag: str
    metrics_etag: str


@dataclass(frozen=True)
class Claim:
    """One call's right to load a month, until it records the result or the claim runs out"""

    files: MonthFiles
    claim_id: str
    started_at: str


class Ledger:
    """bike_loads: per month, the two files it was loaded from, or the call loading it"""

    def __init__(self, table: Any) -> None:  # the boto3 Table resource of bike_loads
        self._table = table

    def claim(self, files: MonthFiles, now: datetime, hold: timedelta) -> Claim | None:
        """Claim the month to load it from files"""
        claim = Claim(files, claim_id=uuid.uuid4().hex, started_at=stamp(now))
        free = Attr(LOADS.partition_key).not_exists() | (
            Attr("claimed_until").lte(stamp(now))
            & (
                Attr("load_state").ne(LOADED)
                | Attr("raw_etag").ne(files.raw_etag)
                | Attr("metrics_etag").ne(files.metrics_etag)
            )
        )
        item = self._item(claim, LOADING, claimed_until=stamp(now + hold))
        return claim if self._put(item, free) else None

    def is_loaded(self, files: MonthFiles) -> bool:
        """Whether the month's last load finished, and was from exactly these files"""
        key = {LOADS.partition_key: files.month}
        item = self._table.get_item(Key=key, ConsistentRead=True).get("Item")
        return (
            item is not None
            and item.get("load_state") == LOADED
            and item.get("raw_etag") == files.raw_etag
            and item.get("metrics_etag") == files.metrics_etag
        )

    def finish(self, claim: Claim, now: datetime, counts: Mapping[str, int | Decimal]) -> bool:
        """Record a finished load with its counts; False if the claim was no longer held"""
        item = self._item(claim, LOADED, claimed_until=stamp(now), finished_at=stamp(now), **counts)
        return self._put(item, Attr("claim_id").eq(claim.claim_id))

    def fail(self, claim: Claim, now: datetime, error: str) -> bool:
        """Record a failed load, which frees the month for the message's next delivery"""
        item = self._item(
            claim,
            FAILED,
            claimed_until=stamp(now),
            finished_at=stamp(now),
            load_error=error[:MAX_ERROR_LENGTH],
        )
        return self._put(item, Attr("claim_id").eq(claim.claim_id))

    def forget(self, month: str, now: datetime) -> bool:
        """Drop the month's item, so its next event loads it again; False while it is claimed"""
        unclaimed = (
            Attr(LOADS.partition_key).not_exists()
            | Attr("load_state").ne(LOADING)
            | Attr("claimed_until").lte(stamp(now))
        )
        key = {LOADS.partition_key: month}
        return conditionally(self._table.delete_item, Key=key, ConditionExpression=unclaimed)

    def entries(self) -> list[Item]:
        """Every month's item, the earliest month first"""
        return sorted(all_items(self._table.scan), key=lambda item: item[LOADS.partition_key])

    @staticmethod
    def _item(claim: Claim, state: str, **attributes: Any) -> Item:
        return {
            LOADS.partition_key: claim.files.month,
            "load_state": state,
            "raw_etag": claim.files.raw_etag,
            "metrics_etag": claim.files.metrics_etag,
            "claim_id": claim.claim_id,
            "started_at": claim.started_at,
            **attributes,
        }

    def _put(self, item: Item, condition: ConditionBase) -> bool:
        """Put item if condition holds, False if it does not"""
        return conditionally(self._table.put_item, Item=item, ConditionExpression=condition)


# --- data tables


def key_of(spec: TableSpec, item: Item) -> tuple[str, str]:
    return item[spec.partition_key], item[spec.sort_key]


def split(items: Sequence[Item], parts: int) -> list[Sequence[Item]]:
    """items as at most parts runs of about the same length, in order"""
    size = max(1, math.ceil(len(items) / parts))
    return [items[start : start + size] for start in range(0, len(items), size)]


class Writer:
    """Writes a month's items and removes whatever an earlier load of it left behind"""

    def __init__(self, connect: Callable[[], Any], threads: int = WRITE_THREADS) -> None:
        self._connect = connect
        self._threads = threads
        self._local = threading.local()

    def replace(self, part: TableSlice) -> int:
        """Put part.items, then delete every other item in part.partitions"""
        spec = part.spec
        found = self._map(partial(self._keys, spec), part.partitions)
        before: set[tuple[str, str]] = set().union(*found)
        stale = sorted(before - {key_of(spec, item) for item in part.items})
        parts = max(1, min(self._threads, len(part.items) // ITEMS_PER_THREAD))
        self._map(partial(self._put, spec), split(part.items, parts))
        if stale:
            with self._table(spec).batch_writer() as batch:
                for partition, sort in stale:
                    batch.delete_item(Key={spec.partition_key: partition, spec.sort_key: sort})
        return len(stale)

    def _map(self, work: Callable[[Any], Any], values: Sequence[Any]) -> list[Any]:
        """work on each value: in this thread for one value, on a thread pool for more"""
        if len(values) <= 1:
            return [work(value) for value in values]
        with ThreadPoolExecutor(max_workers=min(self._threads, len(values))) as pool:
            return list(pool.map(work, values))

    def _table(self, spec: TableSpec) -> Any:
        """spec's table, through the calling thread's own resource"""
        if not hasattr(self._local, "dynamodb"):
            self._local.dynamodb = self._connect()
        return self._local.dynamodb.Table(spec.name)

    def _keys(self, spec: TableSpec, partition: str) -> set[tuple[str, str]]:
        """The keys of the items in one partition of spec's table"""
        items = all_items(
            self._table(spec).query,
            KeyConditionExpression=Key(spec.partition_key).eq(partition),
            ProjectionExpression=f"{spec.partition_key}, {spec.sort_key}",
            ConsistentRead=True,
        )
        return {key_of(spec, item) for item in items}

    def _put(self, spec: TableSpec, items: Sequence[Item]) -> None:
        """Put a run of items into spec's table, 25 to a request"""
        with self._table(spec).batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)


# --- SQS


class Queue:
    """The queue the trigger reads, for the one thing the handler does to it itself"""

    def __init__(self, sqs: Any) -> None:
        self._sqs = sqs
        self._urls: dict[str, str] = {}

    def retry_in(self, queue_arn: str, receipt_handle: str, seconds: int) -> None:
        """Have a message come back in seconds rather than after the visibility timeout"""
        self._sqs.change_message_visibility(
            QueueUrl=self._url(queue_arn), ReceiptHandle=receipt_handle, VisibilityTimeout=seconds
        )

    def _url(self, queue_arn: str) -> str:
        if queue_arn not in self._urls:
            *_, account, name = queue_arn.split(":")
            response = self._sqs.get_queue_url(QueueName=name, QueueOwnerAWSAccountId=account)
            self._urls[queue_arn] = response["QueueUrl"]
        return self._urls[queue_arn]
