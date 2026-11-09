from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import ClientError

from bikes.loader.month import TableSlice
from bikes.loader.stores import (
    FAILED,
    LOADED,
    LOADING,
    Ledger,
    MonthFiles,
    Queue,
    S3Files,
    Writer,
    split,
    stamp,
)
from bikes.resources import BUCKET, DAILY_METRICS, LOADS, MONTHLY_METRICS, QUEUE, TRIPS
from fakes import FakeDynamoDB, FakeS3, FakeSQS, FakeTable, client_error

KEY = "raw/year=2019/month=06/trips_2019-06.csv"
NOON = datetime(2019, 7, 1, 12, 0, tzinfo=UTC)
HOLD = timedelta(seconds=300)
FILES = MonthFiles("2019-06", raw_etag="raw-1", metrics_etag="metrics-1")


# --- S3Files


@pytest.fixture
def s3():
    s3 = FakeS3()
    s3.create_bucket(Bucket=BUCKET)
    return s3


def test_etag_names_the_version_and_none_means_missing(s3):
    files = S3Files(s3)
    assert files.etag(KEY) is None

    s3.put_object(Bucket=BUCKET, Key=KEY + ".tmp", Body=b"other")  # same prefix, other file
    assert files.etag(KEY) is None
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=b"first")
    first = files.etag(KEY)
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=b"second")

    assert first is not None
    assert files.etag(KEY) not in (None, first)
    assert '"' not in first


def test_a_file_replaced_after_its_etag_was_read_is_not_read(s3):
    files = S3Files(s3)
    s3.put_object(Bucket=BUCKET, Key=KEY, Body="Kamppi, Töölö".encode())
    etag = files.etag(KEY)
    assert files.read_text(KEY, etag) == "Kamppi, Töölö"

    s3.put_object(Bucket=BUCKET, Key=KEY, Body=b"newer")

    with pytest.raises(ClientError, match="PreconditionFailed"):
        files.read_text(KEY, etag)


# --- Ledger


@pytest.fixture
def ledger():
    return Ledger(FakeTable(LOADS))


def entry(ledger: Ledger) -> dict:
    (item,) = ledger.entries()
    return item


def test_a_month_is_claimed_then_recorded_as_loaded(ledger):
    claim = ledger.claim(FILES, NOON, HOLD)
    assert claim is not None
    assert entry(ledger)["load_state"] == LOADING
    assert entry(ledger)["claimed_until"] == stamp(NOON + HOLD)

    assert ledger.finish(claim, NOON + timedelta(seconds=40), {"trips": 715_131})

    item = entry(ledger)
    assert (item["load_state"], item["trips"]) == (LOADED, 715_131)
    assert (item["raw_etag"], item["metrics_etag"]) == ("raw-1", "metrics-1")
    assert item["started_at"] == "2019-07-01T12:00:00+00:00"
    assert item["finished_at"] == "2019-07-01T12:00:40+00:00"
    assert ledger.is_loaded(FILES)


def test_a_month_being_loaded_cannot_be_claimed_again(ledger):
    ledger.claim(FILES, NOON, HOLD)

    later = NOON + timedelta(seconds=10)
    assert ledger.claim(FILES, later, HOLD) is None
    assert ledger.claim(MonthFiles("2019-06", "raw-2", "metrics-1"), later, HOLD) is None
    assert not ledger.is_loaded(FILES)


def test_a_loaded_month_is_claimed_again_only_for_other_files(ledger):
    ledger.finish(ledger.claim(FILES, NOON, HOLD), NOON, {})

    assert ledger.claim(FILES, NOON + timedelta(hours=1), HOLD) is None
    assert ledger.claim(MonthFiles("2019-06", "raw-1", "metrics-2"), NOON, HOLD) is not None


def test_a_failed_load_frees_the_month_and_keeps_the_error(ledger):
    claim = ledger.claim(FILES, NOON, HOLD)

    assert ledger.fail(claim, NOON + timedelta(seconds=5), "ValueError: " + "x" * 2000)

    item = entry(ledger)
    assert item["load_state"] == FAILED
    assert len(item["load_error"]) == 1000
    assert ledger.claim(FILES, NOON + timedelta(seconds=5), HOLD) is not None


def test_the_claim_of_a_call_that_died_runs_out(ledger):
    ledger.claim(FILES, NOON, HOLD)  # and nothing more

    assert ledger.claim(FILES, NOON + HOLD - timedelta(seconds=1), HOLD) is None
    assert ledger.claim(FILES, NOON + HOLD, HOLD) is not None


def test_a_call_that_lost_its_claim_cannot_record_anything(ledger):
    first = ledger.claim(FILES, NOON, HOLD)
    second = ledger.claim(FILES, NOON + HOLD, HOLD)

    assert not ledger.finish(first, NOON + HOLD, {})
    assert not ledger.fail(first, NOON + HOLD, "too late")
    assert ledger.finish(second, NOON + HOLD, {})


def test_a_forgotten_month_is_claimed_like_a_new_one(ledger):
    ledger.finish(ledger.claim(FILES, NOON, HOLD), NOON, {})

    assert ledger.forget("2019-06", NOON)

    assert ledger.entries() == []
    assert ledger.claim(FILES, NOON, HOLD) is not None
    assert ledger.forget("2016-05", NOON)  # nothing to forget is fine too


def test_a_month_being_loaded_is_not_forgotten_until_its_claim_runs_out(ledger):
    ledger.claim(FILES, NOON, HOLD)

    assert not ledger.forget("2019-06", NOON + timedelta(seconds=10))
    assert entry(ledger)["load_state"] == LOADING
    assert ledger.forget("2019-06", NOON + HOLD)


def test_entries_come_in_month_order_across_pages():
    table = FakeTable(LOADS, page_size=2)
    ledger = Ledger(table)
    for month in ("2016-07", "2019-06", "2016-05", "2020-10", "2017-01"):
        ledger.claim(MonthFiles(month, "r", "m"), NOON, HOLD)

    assert [item["trip_month"] for item in ledger.entries()] == [
        "2016-05",
        "2016-07",
        "2017-01",
        "2019-06",
        "2020-10",
    ]


# --- Writer


def daily(day: str, trips: int = 1) -> dict:
    return {"trip_month": day[:7], "trip_date": day, "trips": trips}


def test_writing_a_month_again_removes_what_its_new_files_lack():
    dynamodb = FakeDynamoDB([DAILY_METRICS], page_size=2)  # several pages per partition
    writer = Writer(lambda: dynamodb)
    old = [daily(f"2019-06-{day:02d}") for day in range(1, 6)]
    other_month = daily("2019-07-01")
    writer.replace(TableSlice(DAILY_METRICS, [*old, other_month], ("2019-06", "2019-07")))

    removed = writer.replace(TableSlice(DAILY_METRICS, [daily("2019-06-02", 7)], ("2019-06",)))

    assert removed == 4
    assert sorted(item["trip_date"] for item in dynamodb.items(DAILY_METRICS)) == [
        "2019-06-02",
        "2019-07-01",  # another month's partition is not touched
    ]
    assert dynamodb.Table(DAILY_METRICS.name).items[("2019-06", "2019-06-02")]["trips"] == 7


def test_loading_the_same_items_again_removes_nothing():
    dynamodb = FakeDynamoDB([DAILY_METRICS])
    writer = Writer(lambda: dynamodb)
    part = TableSlice(DAILY_METRICS, [daily("2019-06-01"), daily("2019-06-02")], ("2019-06",))
    writer.replace(part)

    assert writer.replace(part) == 0
    assert len(list(dynamodb.items(DAILY_METRICS))) == 2


def test_a_table_without_owned_partitions_is_only_written():
    dynamodb = FakeDynamoDB([MONTHLY_METRICS])
    writer = Writer(lambda: dynamodb)
    june = {"trip_year": "2019", "trip_month": "2019-06", "trips": 1}
    july = {"trip_year": "2019", "trip_month": "2019-07", "trips": 2}
    writer.replace(TableSlice(MONTHLY_METRICS, [june], ()))

    assert writer.replace(TableSlice(MONTHLY_METRICS, [july], ())) == 0
    assert len(list(dynamodb.items(MONTHLY_METRICS))) == 2


class Connections:
    """A connect function that notes which thread asked for each resource"""

    def __init__(self, dynamodb: FakeDynamoDB) -> None:
        self.dynamodb = dynamodb
        self.threads: list[str] = []
        self._lock = threading.Lock()

    def __call__(self) -> FakeDynamoDB:
        with self._lock:
            self.threads.append(threading.current_thread().name)
        return self.dynamodb


JUNE_DAYS = tuple(f"2019-06-{day:02d}" for day in range(1, 31))


def raw_items(count: int) -> list[dict]:
    return [
        {"trip_date": JUNE_DAYS[n % 30], "station_part": f"Station {n}#0", "trip_count": 1}
        for n in range(count)
    ]


def test_a_big_slice_is_written_by_several_threads_with_a_resource_each():
    dynamodb = FakeDynamoDB([TRIPS], page_size=7)
    connect = Connections(dynamodb)
    writer = Writer(connect, threads=4)
    writer.replace(TableSlice(TRIPS, raw_items(1_000), JUNE_DAYS))

    removed = writer.replace(TableSlice(TRIPS, raw_items(990), JUNE_DAYS))

    assert removed == 10
    assert len(list(dynamodb.items(TRIPS))) == 990
    assert len(connect.threads) > 1
    assert len(set(connect.threads)) == len(connect.threads)  # never two for one thread


def test_a_small_slice_is_written_by_the_calling_thread_alone():
    dynamodb = FakeDynamoDB([DAILY_METRICS])
    connect = Connections(dynamodb)
    writer = Writer(connect)
    part = TableSlice(DAILY_METRICS, [daily("2019-06-01"), daily("2019-06-02")], ("2019-06",))

    writer.replace(part)
    writer.replace(part)

    assert connect.threads == [threading.current_thread().name]


def test_a_failed_write_in_any_thread_fails_the_whole_slice():
    class FailingTable(FakeTable):
        def batch_writer(self):
            raise client_error("ProvisionedThroughputExceededException", "BatchWriteItem")

    dynamodb = FakeDynamoDB([TRIPS])
    dynamodb.tables[TRIPS.name] = FailingTable(TRIPS)

    with pytest.raises(ClientError, match="ProvisionedThroughputExceeded"):
        Writer(lambda: dynamodb, threads=4).replace(TableSlice(TRIPS, raw_items(1_000), ()))


def test_items_are_split_into_runs_in_order():
    assert split(list(range(10)), 3) == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]
    assert split([1], 8) == [[1]]
    assert split([], 8) == []


# --- Queue


def test_a_message_comes_back_after_the_given_seconds():
    sqs = FakeSQS()
    sqs.create_queue(QueueName=QUEUE)
    sqs.send(QUEUE, "event")
    (message,) = sqs.receive_message(QueueUrl=sqs.url(QUEUE))["Messages"]
    queue = Queue(sqs)

    queue.retry_in(sqs.arn(QUEUE), message["ReceiptHandle"], 30)

    assert sqs.release_delayed(QUEUE) == [30]
