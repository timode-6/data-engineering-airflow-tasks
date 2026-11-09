"""The probe's own logic, against fake clients; the real check is running it on LocalStack"""

from __future__ import annotations

import csv
import gzip
import io
import json
import zipfile

import pytest

from bikes import probe
from bikes.probe import (
    Health,
    Layout,
    Progress,
    attribute_name,
    check_lambda,
    compare_layouts,
    fetch_health,
    handler_zip,
    item_bytes,
    measure_writes,
    parse_memory,
    to_item,
    to_station_day_items,
    write_items,
)
from helpers import FIRST_ROW, trip


class Waiter:
    def wait(self, **kwargs):
        pass


class FakeDynamoDB:
    """Keeps items in dicts. Can push back once, or drop the first item of every call"""

    def __init__(self, push_back_once=False, lossy=False):
        self.tables: dict[str, dict] = {}
        self.calls: list[int] = []
        self.deleted: list[str] = []
        self._push_back = push_back_once
        self._lossy = lossy

    def create_table(self, **kwargs):
        self.tables[kwargs["TableName"]] = {}

    def get_waiter(self, name):
        return Waiter()

    def batch_write_item(self, **kwargs):
        ((table, requests),) = kwargs["RequestItems"].items()
        self.calls.append(len(requests))
        accepted, unprocessed = requests, []
        if self._push_back:
            self._push_back = False
            accepted, unprocessed = requests[:-1], requests[-1:]
        if self._lossy:
            accepted = accepted[1:]
        for request in accepted:
            item = request["PutRequest"]["Item"]
            self.tables[table][(item["pk"]["S"], item["sk"]["S"])] = item
        return {"UnprocessedItems": {table: unprocessed} if unprocessed else {}}

    def get_paginator(self, name):
        tables = self.tables

        class Scan:
            def paginate(self, **kwargs):
                yield {"Count": len(tables[kwargs["TableName"]])}

        return Scan()

    def delete_table(self, **kwargs):
        self.deleted.append(kwargs["TableName"])


class FakeLambda:
    def __init__(self, error=False):
        self.created: dict = {}
        self.deleted: list[str] = []
        self._error = error

    def create_function(self, **kwargs):
        self.created = kwargs

    def get_waiter(self, name):
        assert name == "function_active_v2"
        return Waiter()

    def invoke(self, **kwargs):
        if self._error:
            body = {"errorMessage": "boom"}
            return {"FunctionError": "Unhandled", "Payload": io.BytesIO(json.dumps(body).encode())}
        body = {"python": "3.12.11", "boto3": "1.38.0", "buckets": 0, "tables": 0}
        return {"Payload": io.BytesIO(json.dumps(body).encode())}

    def delete_function(self, **kwargs):
        self.deleted.append(kwargs["FunctionName"])


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(probe.time, "sleep", lambda seconds: None)


def items(count: int) -> list[dict]:
    return [
        to_item(trip(f"2019-06-01 08:{n // 60 % 60:02d}:{n % 60:02d}"), n) for n in range(count)
    ]


def test_a_trip_becomes_a_typed_item_keyed_like_the_trips_table():
    item = to_item(FIRST_ROW, 7)

    assert item["pk"] == {"S": "2020-03-23"}
    assert item["sk"] == {"S": "06:09:44#0000007"}
    assert item["return_id"] == {"N": "111.0"}
    assert item["departure_name"] == {"S": "Kuusitie"}
    assert item["avg_speed_km_h"] == {"N": "0.2613965087281795"}


def test_letter_ids_stay_text_and_empty_fields_are_left_out():
    item = to_item(trip("2016-05-02 09:31:00", departure_id="A10", temperature=""), 0)

    assert item["departure_id"] == {"S": "A10"}
    assert "air_temperature_degc" not in item


@pytest.mark.parametrize(
    ("column", "name"),
    [
        ("distance (m)", "distance_m"),
        ("duration (sec.)", "duration_sec"),
        ("Air temperature (degC)", "air_temperature_degc"),
    ],
)
def test_column_names_become_plain_attribute_names(column, name):
    assert attribute_name(column) == name


def test_items_go_25_at_a_time_and_pushed_back_ones_are_resent():
    client = FakeDynamoDB(push_back_once=True)
    client.create_table(TableName="t")

    calls = write_items(client, "t", items(30))

    assert client.calls == [25, 1, 5]  # 24 taken, 1 resent, then the last 5
    assert calls == 3
    assert len(client.tables["t"]) == 30


@pytest.mark.parametrize("threads", [1, 4])
def test_measuring_writes_every_item_and_drops_the_table(threads):
    client = FakeDynamoDB()

    measure_writes(client, items(100), threads)

    (table,) = client.tables
    assert len(client.tables[table]) == 100
    assert client.deleted == [table]


def test_a_run_that_loses_items_fails_and_still_drops_the_table():
    client = FakeDynamoDB(lossy=True)

    with pytest.raises(RuntimeError, match="wrote 100 items but the table holds 96"):
        measure_writes(client, items(100), 1)
    assert len(client.deleted) == 1


def test_health_names_the_services_that_are_not_ready():
    health = Health.from_json(
        {
            "version": "4.14.0",
            "services": {"s3": "running", "sqs": "available", "lambda": "disabled"},
        }
    )

    assert health.not_ready() == ["lambda", "dynamodb"]


def test_the_handler_package_holds_valid_python():
    with zipfile.ZipFile(io.BytesIO(handler_zip())) as archive:
        source = archive.read("probe_handler.py").decode()

    compile(source, "probe_handler.py", "exec")
    assert "def handler(event, context):" in source


def test_lambda_check_calls_twice_and_deletes_the_function():
    client = FakeLambda()

    invocations = check_lambda(client)

    assert [call.answer["python"] for call in invocations] == ["3.12.11", "3.12.11"]
    assert client.created["Runtime"] == "python3.12"
    assert client.deleted == [client.created["FunctionName"]]


def test_a_failing_function_is_reported_and_still_deleted():
    client = FakeLambda(error=True)

    with pytest.raises(RuntimeError, match="boom"):
        check_lambda(client)
    assert client.deleted == [client.created["FunctionName"]]


def test_progress_counts_what_dynamodb_accepted():
    client = FakeDynamoDB(push_back_once=True)
    client.create_table(TableName="t")

    with Progress(30, out=io.StringIO()) as progress:
        write_items(client, "t", items(30), progress)

    assert progress.done == 30


@pytest.mark.parametrize(
    ("usage", "expected"),
    [
        ("512MiB / 31.2GiB", 512 * 2**20),
        ("1.5GiB / 31.2GiB", int(1.5 * 2**30)),
        ("800kB / 1GB", 800_000),
        ("12B / 1GiB", 12),
    ],
)
def test_docker_stats_memory_is_read_in_bytes(usage, expected):
    assert parse_memory(usage) == expected


def test_docker_stats_output_it_cannot_read_raises():
    with pytest.raises(ValueError, match="cannot read"):
        parse_memory("--")


def test_trips_group_into_one_item_per_day_and_station():
    rows = [
        trip("2019-06-01 08:00:00", departure_name="Kamppi (M)"),
        trip("2019-06-01 09:00:00", departure_name="Kamppi (M)"),
        trip("2019-06-01 09:30:00", departure_name="Töölönkatu"),
        trip("2019-06-02 08:00:00", departure_name="Kamppi (M)"),
    ]

    items = to_station_day_items(rows)

    keys = [(item["pk"]["S"], item["sk"]["S"], item["trip_count"]["N"]) for item in items]
    assert keys == [
        ("2019-06-01", "Kamppi (M)#0", "2"),
        ("2019-06-01", "Töölönkatu#0", "1"),
        ("2019-06-02", "Kamppi (M)#0", "1"),
    ]
    lines = [element["S"] for element in items[0]["trips"]["L"]]
    assert list(csv.reader(lines)) == [list(rows[0].values()), list(rows[1].values())]


def test_a_crowded_station_day_is_cut_into_parts():
    rows = [trip(f"2019-06-01 08:00:{second:02d}") for second in range(5)]

    items = to_station_day_items(rows, max_trips=2)

    assert [item["sk"]["S"] for item in items] == ["Kuusitie#0", "Kuusitie#1", "Kuusitie#2"]
    assert [item["trip_count"]["N"] for item in items] == ["2", "2", "1"]


def test_compressed_items_hold_the_same_lines():
    rows = [trip("2019-06-01 08:00:00"), trip("2019-06-01 08:05:00", return_name='Kamppi, "M"')]

    (plain,) = to_station_day_items(rows)
    (packed,) = to_station_day_items(rows, compress=True)

    lines = [element["S"] for element in plain["trips"]["L"]]
    assert gzip.decompress(packed["trips"]["B"]).decode().split("\n") == lines
    assert item_bytes(packed) < item_bytes(plain)


def test_layouts_are_charged_their_own_growth_and_every_table_is_dropped():
    client = FakeDynamoDB()
    small = Layout("small", items(10), trips=10)
    large = Layout("large", items(40), trips=40)
    # warm-up, then before/after for each layout
    readings = iter([1_000, 1_000 + 10 * 500, 6_000, 6_000 + 40 * 250])

    results = compare_layouts(client, [small, large], lambda: next(readings), warm_up=items(5))

    assert [result.bytes_per_trip for result in results] == [500, 250]
    assert len(client.deleted) == 3  # the warm-up table too


def test_without_docker_the_layouts_still_write_but_report_no_memory():
    client = FakeDynamoDB()

    (result,) = compare_layouts(client, [Layout("one", items(10), trips=10)], lambda: None)

    assert result.bytes_per_trip is None
    assert len(client.deleted) == 1


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


def test_health_check_waits_while_localstack_starts(monkeypatch):
    answers = [ConnectionResetError(104, "reset"), ConnectionRefusedError(111, "refused")]

    def urlopen(url, timeout):
        if answers:
            raise answers.pop(0)
        return FakeResponse(json.dumps({"version": "4.14.0", "services": {}}).encode())

    monkeypatch.setattr(probe.urllib.request, "urlopen", urlopen)

    assert fetch_health("http://localhost:4566").version == "4.14.0"


def test_health_check_gives_up_with_a_hint(monkeypatch):
    def urlopen(url, timeout):
        raise ConnectionRefusedError(111, "refused")

    monkeypatch.setattr(probe.urllib.request, "urlopen", urlopen)

    with pytest.raises(SystemExit, match="docker compose up -d localstack"):
        fetch_health("http://localhost:4566", wait_seconds=0)
