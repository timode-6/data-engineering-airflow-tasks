"""The loader Lambda on in-memory S3, SQS and DynamoDB"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from bikes import infra
from bikes.loader import handler
from bikes.loader.handler import WAIT_SECONDS, LoadSummary, MonthLoader
from bikes.loader.stores import FAILED, LOADED, Ledger, MonthFiles, Queue, S3Files, Writer
from bikes.resources import (
    BUCKET,
    LOADS,
    QUEUE,
    STATION_METRICS,
    TABLES,
    TRIPS,
    FileKind,
    S3File,
)
from bikes.s3_events import event_body
from fakes import FakeDynamoDB, FakeS3, FakeSQS
from helpers import counts_text, source_text, trip

VISIBILITY_TIMEOUT = 360
JUNE = [
    trip("2019-06-01 08:00:00", departure_name="Kamppi (M)", return_name="Kuusitie"),
    trip("2019-06-02 09:00:00", departure_name="Kuusitie", return_name="Kamppi (M)"),
]


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


class World:
    """S3 wired to the queue, the five tables, and a loader on them"""

    def __init__(self) -> None:
        self.sqs = FakeSQS()
        self.sqs.create_queue(QueueName=QUEUE)
        self.s3 = FakeS3(notify=self.sqs)
        self.s3.create_bucket(Bucket=BUCKET)
        self.dynamodb = FakeDynamoDB(list(TABLES))
        self.clock = Clock()
        self.loader = MonthLoader(
            files=S3Files(self.s3),
            ledger=Ledger(self.dynamodb.Table(LOADS.name)),
            writer=Writer(lambda: self.dynamodb),
            queue=Queue(self.sqs),
            clock=self.clock,
        )

    def upload(self, kind: FileKind, month: str, text: str) -> None:
        self.s3.put_object(Bucket=BUCKET, Key=S3File(kind, month).key, Body=text.encode())

    def upload_month(self, month: str, rows: list[dict[str, str]]) -> None:
        self.upload(FileKind.RAW, month, source_text(rows))
        self.upload(FileKind.METRICS, month, counts_text(rows))

    def deliver(self) -> list[bool]:
        """Every visible message to the loader, one per call"""
        url, done = self.sqs.url(QUEUE), []
        while messages := self.sqs.receive_message(QueueUrl=url).get("Messages"):
            (message,) = messages
            handle = message["ReceiptHandle"]
            record = {
                "messageId": message["MessageId"],
                "receiptHandle": handle,
                "body": message["Body"],
                "eventSourceARN": self.sqs.arn(QUEUE),
            }
            failures = self.loader.handle_batch({"Records": [record]})["batchItemFailures"]
            done.append(not failures)
            if not failures:
                self.sqs.delete_message(QueueUrl=url, ReceiptHandle=handle)
            elif handle in self.sqs.queues[url]["in_flight"]:  # not delayed
                self.sqs.change_message_visibility(
                    QueueUrl=url, ReceiptHandle=handle, VisibilityTimeout=VISIBILITY_TIMEOUT
                )
        return done

    def load_entry(self, month: str) -> dict:
        return self.dynamodb.Table(LOADS.name).items[(month,)]

    def count(self, spec) -> int:
        return len(list(self.dynamodb.items(spec)))


@pytest.fixture
def world():
    return World()


def test_both_files_events_load_the_month_once(world):
    world.upload_month("2019-06", JUNE)

    assert world.deliver() == [True, True]

    entry = world.load_entry("2019-06")
    assert entry["load_state"] == LOADED
    assert (entry["trips"], entry["days"], entry["stations"], entry["raw_items"]) == (2, 2, 2, 2)
    phases = [entry[name] for name in ("read_seconds", "build_seconds", "write_seconds")]
    assert all(isinstance(seconds, Decimal) for seconds in [*phases, entry["load_seconds"]])
    assert world.count(TRIPS) == 2
    assert world.count(STATION_METRICS) == 2
    assert world.dynamodb.Table(TRIPS.name).writes == 2  # the second event wrote nothing


def test_the_first_file_waits_for_the_second(world):
    world.upload(FileKind.RAW, "2019-06", source_text(JUNE))

    assert world.deliver() == [False]
    assert world.count(TRIPS) == 0
    assert world.sqs.release_delayed(QUEUE) == [WAIT_SECONDS]

    world.upload(FileKind.METRICS, "2019-06", counts_text(JUNE))
    assert world.deliver() == [True, True]  # the trips' event, back, loads
    assert world.load_entry("2019-06")["load_state"] == LOADED


def test_the_counts_can_come_first_too(world):
    world.upload(FileKind.METRICS, "2019-06", counts_text(JUNE))
    assert world.deliver() == [False]

    world.upload(FileKind.RAW, "2019-06", source_text(JUNE))
    world.sqs.release_delayed(QUEUE)

    assert world.deliver() == [True, True]
    assert world.count(TRIPS) == 2


def test_every_month_loads_once_whatever_order_the_events_come_in(world):
    months = {
        "2016-05": [trip("2016-05-31 23:00:00")],
        "2019-06": JUNE,
        "2020-10": [trip("2020-10-01 00:00:00"), trip("2020-10-01 00:01:00")],
    }
    for month, rows in months.items():
        world.upload_month(month, rows)
    world.sqs.queues[world.sqs.url(QUEUE)]["messages"].reverse()

    assert world.deliver() == [True] * 6

    entries = [world.load_entry(month) for month in months]
    assert [entry["load_state"] for entry in entries] == [LOADED] * 3
    assert [entry["trips"] for entry in entries] == [1, 2, 2]


def test_a_failed_load_is_recorded_and_tried_again(world):
    world.upload(FileKind.RAW, "2019-06", source_text(JUNE))
    world.upload(FileKind.METRICS, "2019-06", "name,count\nKamppi (M),1\n")

    assert world.deliver() == [False, False]
    entry = world.load_entry("2019-06")
    assert entry["load_state"] == FAILED
    assert entry["load_error"].startswith("ValueError: unexpected header")

    world.upload(FileKind.METRICS, "2019-06", counts_text(JUNE))  # a fixed file, a new event
    world.sqs.release_delayed(QUEUE)
    assert world.deliver() == [True, True, True]
    assert world.load_entry("2019-06")["load_state"] == LOADED


def test_a_month_another_call_is_loading_waits(world):
    world.upload_month("2019-06", JUNE)
    files = MonthFiles(
        "2019-06",
        world.loader.files.etag(S3File(FileKind.RAW, "2019-06").key),
        world.loader.files.etag(S3File(FileKind.METRICS, "2019-06").key),
    )
    world.loader.ledger.claim(files, world.clock(), timedelta(seconds=300))

    assert world.deliver() == [False, False]
    assert world.sqs.release_delayed(QUEUE) == [WAIT_SECONDS, WAIT_SECONDS]

    world.clock.now += timedelta(seconds=300)
    assert world.deliver() == [True, True]
    assert world.load_entry("2019-06")["load_state"] == LOADED


def test_new_files_load_the_month_again_and_drop_what_is_gone(world):
    world.upload_month("2019-06", JUNE)
    world.deliver()

    fewer = JUNE[:1]
    world.upload_month("2019-06", fewer)
    assert world.deliver() == [True, True]

    entry = world.load_entry("2019-06")
    assert (entry["trips"], entry["removed_items"]) == (1, 2)  # a raw item and a daily item
    assert world.count(TRIPS) == 1


def test_a_reloaded_month_is_loaded_again_from_the_same_files(world):
    world.upload_month("2019-06", JUNE)
    world.deliver()
    first = world.load_entry("2019-06")

    lines: list[str] = []
    queued = infra.reload(
        world.s3, world.sqs, world.loader.ledger, ["2019-06", "2018-01"], report=lines.append
    )

    assert (queued, lines[1]) == (
        1,
        "2018-01: left alone, not in S3: "
        + ", ".join(S3File(kind, "2018-01").key for kind in FileKind),
    )
    assert world.deliver() == [True]
    second = world.load_entry("2019-06")
    assert second["claim_id"] != first["claim_id"]
    assert (second["load_state"], second["removed_items"]) == (LOADED, 0)


def test_a_month_being_loaded_is_not_reloaded_beside_it(world):
    world.upload_month("2019-06", JUNE)
    raw, counts = (world.loader.files.etag(S3File(kind, "2019-06").key) for kind in FileKind)
    world.loader.ledger.claim(
        MonthFiles("2019-06", raw, counts), world.clock(), timedelta(minutes=5)
    )
    lines: list[str] = []

    queued = infra.reload(
        world.s3, world.sqs, world.loader.ledger, ["2019-06"], lines.append, world.clock
    )

    assert (queued, lines) == (0, ["2019-06: left alone, being loaded right now"])
    assert world.load_entry("2019-06")["load_state"] == "loading"


def test_the_log_line_says_where_the_time_went():
    summary = LoadSummary(
        trips=715_131,
        valid_trips=686_427,
        days=30,
        stations=336,
        raw_items=9_906,
        removed_items=0,
        read_seconds=2.04,
        build_seconds=18.31,
        write_seconds=38.82,
    )

    assert str(summary).endswith("in 59.2 s: S3 2.0 s, pandas 18.3 s, DynamoDB 38.8 s")
    counts = summary.counts()
    assert (counts["trips"], counts["load_seconds"]) == (715_131, Decimal("59.2"))
    assert counts["write_seconds"] == Decimal("38.8")


def test_s3s_test_event_and_other_files_are_done_with(world):
    world.sqs.send(QUEUE, json.dumps({"Event": "s3:TestEvent"}))
    world.sqs.send(QUEUE, event_body("raw/readme.txt"))
    world.sqs.send(QUEUE, event_body(S3File(FileKind.RAW, "1999-01").key))  # deleted since

    assert world.deliver() == [True, True, True]
    assert world.count(LOADS) == 0


def test_a_message_that_is_not_json_stays_for_the_dead_letter_queue(world):
    world.sqs.send(QUEUE, "not json")

    assert world.deliver() == [False]
    assert world.sqs.release_delayed(QUEUE) == [VISIBILITY_TIMEOUT]


def test_the_lambda_entry_point_reuses_one_loader(world, monkeypatch):
    monkeypatch.setattr(MonthLoader, "from_aws", classmethod(lambda cls: world.loader))
    handler.default_loader.cache_clear()
    world.upload_month("2019-06", JUNE)
    message = world.sqs.receive_message(QueueUrl=world.sqs.url(QUEUE))["Messages"][0]
    event = {"Records": [{"messageId": "1", "receiptHandle": "1", "body": message["Body"]}]}

    assert handler.handle(event, context=None) == {"batchItemFailures": []}
    assert handler.default_loader() is world.loader
    handler.default_loader.cache_clear()
