from __future__ import annotations

import json
import zipfile
from datetime import UTC, datetime, timedelta

import pytest

from bikes import infra
from bikes.config import AwsSettings
from bikes.infra import CHECK_FILE, CheckError, check, down, loads_table, redrive, status, up
from bikes.loader.stores import Ledger, MonthFiles
from bikes.package import Package, code_sha256
from bikes.resources import (
    BUCKET,
    DEAD_LETTER_QUEUE,
    LOADER_FUNCTION,
    LOADER_ROLE,
    LOADS,
    QUEUE,
    TABLES,
    FileKind,
    S3File,
)
from bikes.s3_events import event_body, object_keys
from fakes import (
    REGION,
    FakeDynamoDB,
    FakeIAM,
    FakeLambda,
    FakeLogs,
    FakeS3,
    FakeSQS,
    FakeSTS,
    FakeTables,
)


@pytest.fixture
def aws():
    sqs = FakeSQS()
    return FakeS3(notify=sqs), sqs, FakeTables()


def run_up(aws, region="eu-north-1") -> list[str]:
    lines: list[str] = []
    up(*aws, region, report=lines.append)
    return lines


def test_up_creates_the_bucket_both_queues_and_five_tables(aws):
    s3, sqs, dynamodb = aws

    run_up(aws)

    assert s3.create_requests == [
        {"Bucket": BUCKET, "CreateBucketConfiguration": {"LocationConstraint": "eu-north-1"}}
    ]
    assert sorted(url.rsplit("/", 1)[-1] for url in sqs.queues) == [QUEUE, DEAD_LETTER_QUEUE]
    assert sorted(dynamodb.tables) == sorted(spec.name for spec in TABLES)
    assert all(request["BillingMode"] == "PAY_PER_REQUEST" for request in dynamodb.created)


def test_the_queue_sends_failures_to_the_dead_letter_queue_and_takes_only_bucket_events(aws):
    _, sqs, _ = aws

    run_up(aws)

    attributes = sqs.queues[sqs.url(QUEUE)]["attributes"]
    redrive = json.loads(attributes["RedrivePolicy"])
    assert redrive == {
        "deadLetterTargetArn": f"arn:aws:sqs:eu-north-1:000000000000:{DEAD_LETTER_QUEUE}",
        "maxReceiveCount": "20",
    }
    assert attributes["VisibilityTimeout"] == "360"
    (statement,) = json.loads(attributes["Policy"])["Statement"]
    assert statement["Resource"] == f"arn:aws:sqs:eu-north-1:000000000000:{QUEUE}"
    assert statement["Condition"] == {"ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{BUCKET}"}}


def test_new_csv_files_under_both_prefixes_notify_the_queue(aws):
    s3, _, _ = aws

    run_up(aws)

    configs = s3.notifications["QueueConfigurations"]
    assert {config["QueueArn"].rsplit(":", 1)[-1] for config in configs} == {QUEUE}
    assert {tuple(config["Events"]) for config in configs} == {("s3:ObjectCreated:*",)}
    filters = [
        {rule["Name"]: rule["Value"] for rule in config["Filter"]["Key"]["FilterRules"]}
        for config in configs
    ]
    assert filters == [
        {"prefix": "raw/", "suffix": ".csv"},
        {"prefix": "metrics/", "suffix": ".csv"},
    ]


def test_running_up_again_creates_nothing_new(aws):
    s3, sqs, dynamodb = aws
    run_up(aws)

    lines = run_up(aws)

    assert len(s3.create_requests) == 1
    assert len(sqs.queues) == 2
    assert len(dynamodb.created) == len(TABLES)
    assert not any("created" in line for line in lines)


def test_us_east_1_gets_no_location_constraint(aws):
    s3, _, _ = aws

    run_up(aws, region="us-east-1")

    assert s3.create_requests == [{"Bucket": BUCKET}]


def test_a_table_with_other_keys_is_an_error_not_a_quiet_fix(aws):
    _, _, dynamodb = aws
    dynamodb.tables[TABLES[0].name] = [{"AttributeName": "id", "KeyType": "HASH"}]

    with pytest.raises(RuntimeError, match="exists with keys"):
        run_up(aws)


def test_check_finds_its_event_and_puts_other_messages_back(aws, monkeypatch):
    s3, sqs, _ = aws
    run_up(aws)
    other = event_body("raw/year=2019/month=06/trips_2019-06.csv")
    sqs.send(QUEUE, json.dumps({"Event": "s3:TestEvent"}))
    sqs.send(QUEUE, other)

    seconds = check(s3, sqs)

    assert seconds >= 0
    assert sqs.queues[sqs.url(QUEUE)]["messages"] == [other]
    assert CHECK_FILE.key not in s3.buckets[BUCKET]


def test_check_fails_if_no_event_arrives_and_still_removes_its_file():
    sqs = FakeSQS()
    s3 = FakeS3()  # not wired to the queue
    run_up((s3, sqs, FakeTables()))

    with pytest.raises(CheckError, match="no event"):
        check(s3, sqs, wait_seconds=0.05)
    assert s3.buckets[BUCKET] == {}


def test_check_needs_the_queue(aws):
    s3, sqs, _ = aws

    with pytest.raises(CheckError, match="bikes.infra up"):
        check(s3, sqs)


def test_redrive_moves_every_dead_letter_back(aws):
    _, sqs, _ = aws
    run_up(aws)
    keys = [f"raw/year=2019/month={n:02d}/trips_2019-{n:02d}.csv" for n in range(1, 13)]
    bodies = [event_body(key) for key in keys]
    for body in bodies:
        sqs.send(DEAD_LETTER_QUEUE, body)

    moved = redrive(sqs, sqs.url(DEAD_LETTER_QUEUE), sqs.url(QUEUE))

    assert moved == 12
    assert sqs.queues[sqs.url(QUEUE)]["messages"] == bodies
    dead_letters = sqs.queues[sqs.url(DEAD_LETTER_QUEUE)]
    assert (dead_letters["messages"], dead_letters["in_flight"]) == ([], {})


def test_down_removes_everything_including_the_files(aws):
    s3, sqs, dynamodb = aws
    run_up(aws)
    s3.put_object(Bucket=BUCKET, Key="raw/year=2019/month=06/trips_2019-06.csv", Body=b"x")
    lines: list[str] = []

    down(s3, sqs, dynamodb, report=lines.append)

    assert (s3.buckets, sqs.queues, dynamodb.tables) == ({}, {}, {})
    assert lines[-1] == f"S3 bucket {BUCKET}: deleted with 1 objects"


def test_down_asks_first(monkeypatch, capsys):
    monkeypatch.setattr(infra.AwsSettings, "from_env", classmethod(lambda cls: None))
    monkeypatch.setattr(infra.aws, "client", lambda service: None)
    monkeypatch.setattr("builtins.input", lambda question: "n")

    infra.main(["down"])

    assert "Nothing deleted." in capsys.readouterr().out


def test_status_counts_files_messages_and_items(aws):
    s3, sqs, dynamodb = aws
    run_up(aws)
    s3.put_object(Bucket=BUCKET, Key="raw/year=2019/month=06/trips_2019-06.csv", Body=b"x" * 2**20)
    lines: list[str] = []

    status(s3, sqs, dynamodb, report=lines.append)

    assert lines[:4] == [
        f"S3 {BUCKET}/raw/: 1 files, 1 MiB",
        f"S3 {BUCKET}/metrics/: 0 files, 0 MiB",
        f"SQS {QUEUE}: 1 waiting, 0 being handled",  # the upload's event
        f"SQS {DEAD_LETTER_QUEUE}: 0 waiting, 0 being handled",
    ]
    assert lines[4:] == [f"DynamoDB {spec.name}: 0 items" for spec in TABLES]


def test_status_says_what_is_missing(aws):
    lines: list[str] = []

    status(*aws, report=lines.append)

    assert lines[0] == f"S3 bucket {BUCKET}: missing, run `python -m bikes.infra up`"
    assert all(line.endswith("missing") for line in lines[1:])


# --- the command line, on fakes


class Cloud:
    """One fake of every service the commands use, wired in place of boto3"""

    def __init__(self, monkeypatch, tmp_path) -> None:
        self.sqs = FakeSQS()
        self.s3 = FakeS3(notify=self.sqs)
        self.clients = {
            "s3": self.s3,
            "sqs": self.sqs,
            "dynamodb": FakeTables(),
            "lambda": FakeLambda(self.s3),
            "iam": FakeIAM(),
            "logs": FakeLogs(),
            "sts": FakeSTS(),
        }
        self.tables = FakeDynamoDB(list(TABLES))
        package = tmp_path / "bike-loader.zip"
        with zipfile.ZipFile(package, "w") as archive:
            archive.writestr("bikes/__init__.py", "")
        self.package = Package(package, "x86_64", code_sha256(package), files=1)
        settings = AwsSettings(endpoint_url="http://localhost:4566", region=REGION)
        monkeypatch.setattr(infra.AwsSettings, "from_env", classmethod(lambda cls: settings))
        monkeypatch.setattr(infra.aws, "client", lambda service: self.clients[service])
        monkeypatch.setattr(infra.aws, "resource", lambda service: self.tables)
        monkeypatch.setattr(infra, "build_package", lambda architecture: self.package)


@pytest.fixture
def cloud(monkeypatch, tmp_path):
    return Cloud(monkeypatch, tmp_path)


def test_deploy_needs_up_first(cloud):
    with pytest.raises(SystemExit, match="Run `python -m bikes.infra up` first"):
        infra.main(["deploy"])


def test_up_deploy_status_and_down(cloud, capsys):
    infra.main(["up"])
    infra.main(["deploy"])

    function = cloud.clients["lambda"].functions[LOADER_FUNCTION]
    assert function["Role"].endswith(f":role/{LOADER_ROLE}")
    (mapping,) = cloud.clients["lambda"].mappings.values()
    assert mapping["EventSourceArn"] == cloud.sqs.arn(QUEUE)
    assert f"SQS {QUEUE} → Lambda {LOADER_FUNCTION}: created" in capsys.readouterr().out

    infra.main(["status"])
    out = capsys.readouterr().out
    assert f"Lambda {LOADER_FUNCTION}: Active; triggered by {QUEUE}, Enabled" in out
    assert "Months: none loaded yet" in out

    infra.main(["down", "--yes"])
    assert cloud.clients["lambda"].functions == {}
    assert cloud.clients["iam"].roles == {}
    assert cloud.s3.buckets == {}


def test_check_steps_aside_once_the_lambda_reads_the_queue(cloud):
    infra.main(["up"])
    infra.main(["deploy"])

    with pytest.raises(SystemExit, match="Not checked"):
        infra.main(["check"])


def test_loads_shows_every_month_and_why_one_failed(cloud, capsys):
    infra.main(["up"])
    ledger = Ledger(cloud.tables.Table(LOADS.name))
    noon = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)
    june = ledger.claim(MonthFiles("2019-06", "r", "m"), noon, timedelta(minutes=5))
    counts = {"trips": 715_131, "valid_trips": 686_427, "days": 30, "stations": 336}
    seconds = {"read_seconds": 2, "build_seconds": 18.3, "write_seconds": 20.7}
    ledger.finish(june, noon, {**counts, "raw_items": 9_906, "load_seconds": 41, **seconds})
    july = ledger.claim(MonthFiles("2019-07", "r", "m"), noon, timedelta(minutes=5))
    ledger.fail(july, noon, "ValueError: unexpected header")
    capsys.readouterr()

    infra.main(["loads"])

    lines = capsys.readouterr().out.splitlines()
    june_line = "2019-06 loaded 715,131 686,427 30 336 9,906 41.0 2.0 18.3 20.7"
    assert " ".join(lines[1].split()[:11]) == june_line
    assert lines[2].split()[:2] == ["2019-07", "failed"]
    assert lines[3].strip() == "ValueError: unexpected header"
    assert lines[-1] == "Months: 1 failed, 1 loaded"


def test_no_loads_is_one_line():
    assert loads_table([]) == ["Months: none loaded yet"]


def test_redrive_from_the_command_line(cloud, capsys):
    infra.main(["up"])
    cloud.sqs.send(DEAD_LETTER_QUEUE, event_body("raw/year=2019/month=06/trips_2019-06.csv"))
    capsys.readouterr()

    infra.main(["redrive"])

    assert capsys.readouterr().out == f"SQS {DEAD_LETTER_QUEUE} → {QUEUE}: moved 1 messages back\n"


def test_reload_takes_months_or_all_but_not_both(cloud):
    infra.main(["up"])

    for argv in (["reload"], ["reload", "2019-06", "--all"]):
        with pytest.raises(SystemExit, match="Name the months"):
            infra.main(argv)
    with pytest.raises(SystemExit):
        infra.main(["reload", "2019-13"])  # argparse refuses it


def test_reload_all_forgets_each_loaded_month_and_queues_its_trips_again(cloud, capsys):
    infra.main(["up"])
    june = S3File(FileKind.RAW, "2019-06")
    for file in (june, june.partner):
        cloud.s3.put_object(Bucket=BUCKET, Key=file.key, Body=file.key.encode())
    queue = cloud.sqs.queues[cloud.sqs.url(QUEUE)]["messages"]
    queue.clear()  # the uploads' own events
    ledger = Ledger(cloud.tables.Table(LOADS.name))
    noon = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)
    for month in ("2019-06", "2019-07"):
        claim = ledger.claim(MonthFiles(month, "r", "m"), noon, timedelta(minutes=5))
        ledger.finish(claim, noon, {})
    capsys.readouterr()

    infra.main(["reload", "--all"])

    out = capsys.readouterr().out
    assert "2019-06: queued" in out
    assert "2019-07: left alone, not in S3" in out
    assert [entry["trip_month"] for entry in ledger.entries()] == ["2019-07"]
    (body,) = queue
    assert object_keys(body) == [june.key]
