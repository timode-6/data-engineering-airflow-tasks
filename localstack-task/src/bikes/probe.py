"""Check that LocalStack does what the pipeline needs, and how fast

    python -m bikes.probe
    python -m bikes.probe --items 100000 --threads 1 8 --skip-lambda
    python -m bikes.probe --layouts --items 100000

With LocalStack up, it checks LocalStack's health, measures DynamoDB batch
writes of real trips, and runs a Lambda that calls S3 and DynamoDB from
inside. --layouts instead compares layouts of the raw trips table by memory
per trip and write speed; run it on a freshly restarted LocalStack. Whatever
the probe creates, it removes
"""

from __future__ import annotations

import argparse
import csv
import functools
import gzip
import io
import itertools
import json
import math
import re
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
import zipfile
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

import boto3
from botocore.config import Config

from bikes.config import AwsSettings, ConfigError
from bikes.dataset import COLUMNS, DEPARTURE, DEPARTURE_NAME
from bikes.package import lambda_architecture

SERVICES = ("s3", "sqs", "lambda", "dynamodb")
READY = ("available", "running")
BATCH_SIZE = 25  # the most one BatchWriteItem call takes
BIGGEST_MONTH_ROWS = 715_131  # 2019-06, from the split
ALL_TRIPS = 12_157_292  # every row the split wrote
DEFAULT_SOURCE = Path("data/incoming/trips_2019-06.csv")
DEFAULT_CONTAINER = "bikes-localstack-1"
MAX_TRIPS_PER_ITEM = 1_000  # about 160 KB of lines, far under DynamoDB's 400 KB item limit
LAYOUT_THREADS = 4
WARM_UP_TRIPS = 5_000
# LocalStack does not check that the role exists; real AWS would though
PROBE_ROLE = "arn:aws:iam::000000000000:role/lambda-role"
# First calls wait while LocalStack starts DynamoDB or a function's container
CLIENT_CONFIG = Config(read_timeout=300, retries={"max_attempts": 3, "mode": "standard"})
MEMORY_UNITS = {
    "B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40,
    "kB": 10**3, "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12,
}  # fmt: skip

PROBE_HANDLER = '''\
import sys
import time

import boto3


def handler(event, context):
    """Report the Python and boto3 versions and what is visible in LocalStack."""
    started = time.monotonic()
    buckets = boto3.client("s3").list_buckets()["Buckets"]
    tables = boto3.client("dynamodb").list_tables()["TableNames"]
    return {
        "python": sys.version.split()[0],
        "boto3": boto3.__version__,
        "buckets": len(buckets),
        "tables": len(tables),
        "seconds": round(time.monotonic() - started, 3),
    }
'''


# --- health


@dataclass(frozen=True)
class Health:
    """What /_localstack/health says"""

    version: str
    edition: str
    services: Mapping[str, str]

    @classmethod
    def from_json(cls, body: Mapping[str, Any]) -> Health:
        return cls(
            version=body.get("version", "?"),
            edition=body.get("edition", "?"),
            services=body.get("services", {}),
        )

    def not_ready(self, names: Sequence[str] = SERVICES) -> list[str]:
        """The services among names that LocalStack does not offer."""
        return [name for name in names if self.services.get(name) not in READY]


def fetch_health(endpoint_url: str, wait_seconds: float = 60) -> Health:
    """Ask LocalStack how it is, waiting while it starts up"""
    url = f"{endpoint_url.rstrip('/')}/_localstack/health"
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                return Health.from_json(json.load(response))
        except OSError as error:
            if time.monotonic() >= deadline:
                raise SystemExit(
                    f"No answer from LocalStack at {url} ({error}).\n"
                    "Is it running? docker compose up -d localstack"
                ) from error
            time.sleep(2)


# --- DynamoDB


def read_rows(path: Path, limit: int) -> list[dict[str, str]]:
    """The first limit rows of a monthly file, as text"""
    with path.open(encoding="utf-8", newline="") as handle:
        return list(itertools.islice(csv.DictReader(handle), limit))


def attribute_name(column: str) -> str:
    """'distance (m)' -> 'distance_m': a name that needs no placeholder in expressions"""
    return re.sub(r"\W+", "_", column).strip("_").lower()


def is_number(value: str) -> bool:
    """True for what DynamoDB accepts as a number (no NaN, no infinity)"""
    try:
        return math.isfinite(float(value))
    except ValueError:
        return False


def to_item(row: Mapping[str, str], number: int) -> dict[str, dict[str, str]]:
    """A trip as a low-level DynamoDB item, keyed by departure; number keeps keys unique"""
    departure = row[DEPARTURE]
    item = {"pk": {"S": departure[:10]}, "sk": {"S": f"{departure[11:]}#{number:07d}"}}
    for column in COLUMNS:
        value = row[column]
        if value:
            item[attribute_name(column)] = {"N": value} if is_number(value) else {"S": value}
    return item


def csv_line(row: Mapping[str, str]) -> str:
    """A row as the CSV line it was read from"""
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="").writerow(row[column] for column in COLUMNS)
    return buffer.getvalue()


def to_station_day_items(
    rows: Sequence[Mapping[str, str]],
    compress: bool = False,
    max_trips: int = MAX_TRIPS_PER_ITEM,
) -> list[dict[str, Any]]:
    """One item per departure day and station, holding its trips as CSV lines"""
    groups: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        key = (row[DEPARTURE][:10], row[DEPARTURE_NAME])
        groups.setdefault(key, []).append(csv_line(row))
    items = []
    for (day, station), lines in groups.items():
        for part, start in enumerate(range(0, len(lines), max_trips)):
            chunk = lines[start : start + max_trips]
            if compress:
                trips = {"B": gzip.compress("\n".join(chunk).encode())}
            else:
                trips = {"L": [{"S": line} for line in chunk]}
            items.append(
                {
                    "pk": {"S": day},
                    "sk": {"S": f"{station}#{part}"},
                    "trip_count": {"N": str(len(chunk))},
                    "trips": trips,
                }
            )
    return items


def value_bytes(value: Mapping[str, Any]) -> int:
    """Rough DynamoDB size of one typed value."""
    ((kind, content),) = value.items()
    if kind == "L":
        return sum(value_bytes(element) for element in content)
    if kind == "B":
        return len(content)
    return len(str(content).encode())


def item_bytes(item: Mapping[str, Mapping[str, Any]]) -> int:
    """Rough DynamoDB size of an item: attribute names plus values"""
    return sum(len(name.encode()) + value_bytes(value) for name, value in item.items())


def size_label(size: float) -> str:
    """'364 bytes' or '21 KB'."""
    return f"{size:,.0f} bytes" if size < 1024 else f"{size / 1024:,.0f} KB"


class Progress:
    """Counts items written by any number of threads, shown on stderr every few seconds"""

    def __init__(self, total: int, out: TextIO = sys.stderr, every: float = 5.0) -> None:
        self.total = total
        self.done = 0
        self._out = out
        self._every = every
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._started = time.monotonic()
        self._thread = threading.Thread(target=self._show, daemon=True)

    def add(self, count: int) -> None:
        with self._lock:
            self.done += count

    def __enter__(self) -> Progress:
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._stop.set()
        self._thread.join()
        print("\r" + " " * 60 + "\r", end="", file=self._out, flush=True)

    def _show(self) -> None:
        while not self._stop.wait(self._every):
            rate = self.done / (time.monotonic() - self._started)
            line = f"\r    {self.done:,} / {self.total:,} items, {rate:,.0f} items/s"
            print(line, end="", file=self._out, flush=True)


def create_table(client: Any) -> str:
    """A throwaway table with the trips table's key shape; returns its name"""
    name = f"bikes-probe-{uuid.uuid4().hex[:8]}"
    client.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    client.get_waiter("table_exists").wait(
        TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 120}
    )
    return name


def write_items(
    client: Any,
    table: str,
    items: Sequence[Mapping[str, Any]],
    progress: Progress | None = None,
) -> int:
    """Write items 25 at a time, resending what DynamoDB leaves unprocessed; returns the calls"""
    calls = 0
    for start in range(0, len(items), BATCH_SIZE):
        pending = [{"PutRequest": {"Item": item}} for item in items[start : start + BATCH_SIZE]]
        attempt = 0
        while pending:
            if attempt:
                time.sleep(min(0.05 * 2**attempt, 2.0))
            response = client.batch_write_item(RequestItems={table: pending})
            calls += 1
            attempt += 1
            unprocessed = response.get("UnprocessedItems", {}).get(table, [])
            if progress is not None:
                progress.add(len(pending) - len(unprocessed))
            pending = unprocessed
    return calls


def count_items(client: Any, table: str) -> int:
    """How many items the table holds, counted with a paginated scan"""
    pages = client.get_paginator("scan").paginate(TableName=table, Select="COUNT")
    return sum(page["Count"] for page in pages)


def write_parallel(
    client: Any, table: str, items: Sequence[Mapping[str, Any]], threads: int
) -> float:
    """Seconds to write items with threads writers, showing progress meanwhile"""
    parts = [items[i::threads] for i in range(threads)]
    started = time.monotonic()
    with Progress(len(items)) as progress, ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(lambda part: write_items(client, table, part, progress), parts))
    return time.monotonic() - started


def measure_writes(client: Any, items: Sequence[Mapping[str, Any]], threads: int) -> float:
    """Seconds to write items into a fresh table, counted afterwards so a lossy run fails"""
    table = create_table(client)
    try:
        elapsed = write_parallel(client, table, items, threads)
        stored = count_items(client, table)
    finally:
        client.delete_table(TableName=table)
    if stored != len(items):
        raise RuntimeError(f"wrote {len(items):,} items but the table holds {stored:,}")
    return elapsed


def parse_memory(usage: str) -> int:
    """Bytes in use from docker stats' MemUsage, e.g. "1.5GiB / 31.2GiB" """
    match = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", usage)
    if not match or match.group(2) not in MEMORY_UNITS:
        raise ValueError(f"cannot read memory usage {usage!r}")
    return round(float(match.group(1)) * MEMORY_UNITS[match.group(2)])


def container_memory(container: str) -> int | None:
    """Memory the container uses right now, or None if docker cannot tell"""
    command = ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
        return parse_memory(result.stdout)
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


@dataclass(frozen=True)
class Layout:
    """One way to store the raw trips: the items it takes, and how many trips"""

    name: str
    items: list[dict[str, Any]]
    trips: int


@dataclass(frozen=True)
class LayoutResult:
    layout: Layout
    grown: int | None  # bytes LocalStack grew by while the layout was written
    seconds: float

    @property
    def bytes_per_trip(self) -> float | None:
        return None if self.grown is None else self.grown / self.layout.trips


def compare_layouts(
    client: Any,
    layouts: Sequence[Layout],
    read_memory: Callable[[], int | None],
    threads: int = LAYOUT_THREADS,
    warm_up: Sequence[Mapping[str, Any]] = (),
) -> list[LayoutResult]:
    """Write each layout into a table of its own and note how much LocalStack grows"""
    tables: list[str] = []
    try:
        if warm_up:
            tables.append(create_table(client))
            write_parallel(client, tables[-1], warm_up, threads)
        results = []
        for layout in layouts:
            tables.append(create_table(client))
            before = read_memory()
            seconds = write_parallel(client, tables[-1], layout.items, threads)
            after = read_memory()
            grown = None if before is None or after is None else after - before
            results.append(LayoutResult(layout=layout, grown=grown, seconds=seconds))
        return results
    finally:
        for table in tables:
            client.delete_table(TableName=table)


# --- Lambda


@dataclass(frozen=True)
class Invocation:
    """One call to the probe function"""

    seconds: float
    answer: Mapping[str, Any]


def handler_zip() -> bytes:
    """The probe function's code as a deployment package"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("probe_handler.py", PROBE_HANDLER)
    return buffer.getvalue()


def check_lambda(client: Any, calls: int = 2) -> list[Invocation]:
    """Create the probe function, call it, delete it"""
    name = f"bikes-probe-{uuid.uuid4().hex[:8]}"
    client.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=PROBE_ROLE,
        Handler="probe_handler.handler",
        Code={"ZipFile": handler_zip()},
        Timeout=60,
        Architectures=[lambda_architecture()],
    )
    try:
        client.get_waiter("function_active_v2").wait(
            FunctionName=name, WaiterConfig={"Delay": 2, "MaxAttempts": 150}
        )
        invocations = []
        for _ in range(calls):
            started = time.monotonic()
            response = client.invoke(FunctionName=name, Payload=b"{}")
            seconds = time.monotonic() - started
            answer = json.loads(response["Payload"].read())
            if "FunctionError" in response:
                raise RuntimeError(f"the probe function failed: {answer}")
            invocations.append(Invocation(seconds=seconds, answer=answer))
        return invocations
    finally:
        client.delete_function(FunctionName=name)


# --- CLI


def report_health(health: Health, endpoint_url: str) -> None:
    print(f"LocalStack {health.version} ({health.edition}) at {endpoint_url}")
    print("  " + " · ".join(f"{name} {health.services.get(name, 'absent')}" for name in SERVICES))
    if missing := health.not_ready():
        raise SystemExit(f"Not available: {', '.join(missing)}. The pipeline needs all four")


def load_rows(args: argparse.Namespace) -> list[dict[str, str]]:
    if not args.source.exists():
        raise SystemExit(f"{args.source} not found: run the monthly split first, or pass --source")
    return read_rows(args.source, args.items)


def report_dynamodb(client: Any, args: argparse.Namespace) -> None:
    items = [to_item(row, number) for number, row in enumerate(load_rows(args))]
    size = sum(item_bytes(item) for item in items) / max(len(items), 1)
    print(f"\nDynamoDB: {len(items):,} trips from {args.source.name}, about {size:.0f} bytes each")
    print("  (the first table can take a while: LocalStack starts DynamoDB on first use)")
    for count in args.threads:
        seconds = measure_writes(client, items, count)
        rate = len(items) / seconds
        print(
            f"  {count:>2} thread{'s' if count > 1 else ' '}  {seconds:6.1f} s  "
            f"{rate:7,.0f} items/s   biggest month ≈ {BIGGEST_MONTH_ROWS / rate / 60:.1f} min, "
            f"all trips ≈ {ALL_TRIPS / rate / 3600:.1f} h"
        )


def report_layouts(client: Any, args: argparse.Namespace) -> None:
    rows = load_rows(args)
    layouts = [
        Layout("one item per trip", [to_item(row, n) for n, row in enumerate(rows)], len(rows)),
        Layout("one item per day and station", to_station_day_items(rows), len(rows)),
        Layout("the same, gzip-compressed", to_station_day_items(rows, compress=True), len(rows)),
    ]
    warm_up = [to_item(row, n) for n, row in enumerate(rows[:WARM_UP_TRIPS])]
    print(
        f"\nRaw trips table layouts: {len(rows):,} trips from {args.source.name}, "
        f"{LAYOUT_THREADS} writer threads"
    )
    reader = functools.partial(container_memory, args.container)
    for result in compare_layouts(client, layouts, reader, warm_up=warm_up):
        layout = result.layout
        sizes = [item_bytes(item) for item in layout.items]
        rate = layout.trips / result.seconds
        print(f"  {layout.name}")
        print(
            f"    {len(layout.items):,} items, {size_label(sum(sizes) / len(sizes))} on average, "
            f"largest {size_label(max(sizes))}; {rate:,.0f} trips/s, "
            f"all trips ≈ {ALL_TRIPS / rate / 60:,.0f} min"
        )
        if result.bytes_per_trip is None:
            print(f"    memory unknown: docker stats could not read {args.container}")
        else:
            total = result.bytes_per_trip * ALL_TRIPS / 2**30
            print(
                f"    LocalStack grew {result.grown / 2**20:,.0f} MiB, "
                f"{result.bytes_per_trip:,.0f} bytes per trip; all trips ≈ {total:.1f} GiB"
            )


def report_lambda(client: Any) -> None:
    print(f"\nLambda: python3.12 on {lambda_architecture()}")
    invocations = check_lambda(client)
    labels = ["first call (starts the container)", "second call (warm)"]
    for label, invocation in zip(labels, invocations, strict=False):
        print(f"  {label:<36} {invocation.seconds:6.2f} s")
    answer = invocations[-1].answer
    print(
        f"  inside: Python {answer['python']}, boto3 {answer['boto3']}; reached S3 "
        f"({answer['buckets']} buckets) and DynamoDB ({answer['tables']} tables)"
    )


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m bikes.probe",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="monthly file to read")
    parser.add_argument("--items", type=int, default=50_000, help="trips to write per run")
    parser.add_argument(
        "--threads", type=int, nargs="+", default=[1, 4, 8], help="writer threads to try"
    )
    parser.add_argument(
        "--layouts",
        action="store_true",
        help="compare raw-table layouts instead (on a freshly restarted LocalStack)",
    )
    parser.add_argument("--container", default=DEFAULT_CONTAINER, help="LocalStack's container")
    parser.add_argument("--skip-dynamodb", action="store_true")
    parser.add_argument("--skip-lambda", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        settings = AwsSettings.from_env()
    except ConfigError as error:
        raise SystemExit(str(error)) from error

    report_health(fetch_health(settings.endpoint_url), settings.endpoint_url)
    if args.layouts:
        report_layouts(boto3.client("dynamodb", config=CLIENT_CONFIG), args)
        return
    if not args.skip_dynamodb:
        report_dynamodb(boto3.client("dynamodb", config=CLIENT_CONFIG), args)
    if not args.skip_lambda:
        report_lambda(boto3.client("lambda", config=CLIENT_CONFIG))


if __name__ == "__main__":
    main()
