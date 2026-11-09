from __future__ import annotations

import json
import zipfile
from datetime import datetime

import pytest

from bikes import deploy
from bikes.deploy import LOG_GROUP, loader_policy, recent_logs, remove, status_lines
from bikes.package import Package, code_sha256
from bikes.resources import (
    BUCKET,
    LOADER_FUNCTION,
    LOADER_PACKAGE_KEY,
    LOADER_ROLE,
    QUEUE,
    TABLES,
)
from fakes import ACCOUNT, REGION, FakeIAM, FakeLambda, FakeLogs, FakeS3, FakeSQS

QUEUE_ARN = FakeSQS().arn(QUEUE)


def make_package(tmp_path, code: str = "def handle(event, context): ...\n") -> Package:
    path = tmp_path / "bike-loader.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("bikes/loader/handler.py", code)
    return Package(path, "x86_64", code_sha256(path), files=1)


class Cloud:
    def __init__(self) -> None:
        self.s3 = FakeS3()
        self.s3.create_bucket(Bucket=BUCKET)
        self.lambda_client = FakeLambda(self.s3)
        self.iam = FakeIAM()
        self.logs = FakeLogs()
        self.lines: list[str] = []

    def deploy(self, package: Package) -> None:
        deploy.deploy(
            self.lambda_client,
            self.iam,
            self.s3,
            package=package,
            queue_arn=QUEUE_ARN,
            policy=loader_policy(ACCOUNT, REGION, QUEUE_ARN),
            report=self.lines.append,
        )

    def function(self) -> dict:
        return self.lambda_client.functions[LOADER_FUNCTION]

    def mapping(self) -> dict:
        (mapping,) = self.lambda_client.mappings.values()
        return mapping


@pytest.fixture
def cloud():
    return Cloud()


def test_deploy_makes_the_role_the_function_and_the_trigger(cloud, tmp_path):
    package = make_package(tmp_path)

    cloud.deploy(package)

    role = cloud.iam.roles[LOADER_ROLE]
    (statement,) = json.loads(role["AssumeRolePolicyDocument"])["Statement"]
    assert statement["Principal"] == {"Service": "lambda.amazonaws.com"}
    function = cloud.function()
    assert function["Role"] == role["Arn"]
    assert function["Handler"] == "bikes.loader.handler.handle"
    assert (function["Runtime"], function["Architectures"]) == ("python3.12", ["x86_64"])
    assert (function["Timeout"], function["MemorySize"]) == (300, 2048)
    assert function["CodeSha256"] == package.sha256
    assert cloud.s3.uploads == [LOADER_PACKAGE_KEY]
    mapping = cloud.mapping()
    assert mapping["EventSourceArn"] == QUEUE_ARN
    assert mapping["BatchSize"] == 1
    assert mapping["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert mapping["ScalingConfig"] == {"MaximumConcurrency": 2}
    assert cloud.lines[-1].startswith(f"SQS {QUEUE} → Lambda {LOADER_FUNCTION}: created, Enabled")


def test_deploying_the_same_package_again_uploads_nothing(cloud, tmp_path):
    package = make_package(tmp_path)
    cloud.deploy(package)

    cloud.deploy(package)

    assert cloud.s3.uploads == [LOADER_PACKAGE_KEY]
    assert "update_function_code" not in cloud.lambda_client.calls
    assert len(cloud.lambda_client.mappings) == 1
    assert "updated, same code" in cloud.lines[-2]


def test_new_code_updates_the_function_in_place(cloud, tmp_path):
    cloud.deploy(make_package(tmp_path))
    newer = make_package(tmp_path, code="def handle(event, context): return {}\n")

    cloud.deploy(newer)

    assert cloud.lambda_client.calls.count("create_function") == 1
    assert cloud.function()["CodeSha256"] == newer.sha256
    assert cloud.lambda_client.waited[-2:] == ["function_updated_v2", "function_updated_v2"]
    assert cloud.mapping()["State"] == "Enabled"


def test_the_trigger_is_waited_for_until_it_is_enabled(cloud):
    sleeps: list[float] = []
    cloud.lambda_client.new_mapping_state = "Creating"

    done, state = deploy.ensure_trigger(cloud.lambda_client, QUEUE_ARN, sleep=sleeps.append)

    assert (done, state, sleeps) == ("created", "Enabled", [1])


def test_the_policy_reaches_the_pipelines_resources_only():
    statements = {s["Sid"]: s for s in loader_policy(ACCOUNT, REGION, QUEUE_ARN)["Statement"]}

    assert statements["ReadMonthFiles"]["Resource"] == [
        f"arn:aws:s3:::{BUCKET}/raw/*",
        f"arn:aws:s3:::{BUCKET}/metrics/*",
    ]
    assert statements["TakeMessages"]["Resource"] == QUEUE_ARN
    assert "sqs:ChangeMessageVisibility" in statements["TakeMessages"]["Action"]
    assert statements["WriteTables"]["Resource"] == [
        f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{spec.name}" for spec in TABLES
    ]
    assert not any(s["Resource"] == "*" for s in statements.values())


def test_status_says_whether_the_function_is_there(cloud, tmp_path):
    assert "not deployed" in status_lines(cloud.lambda_client)[0]

    cloud.deploy(make_package(tmp_path))

    assert status_lines(cloud.lambda_client) == [
        f"Lambda {LOADER_FUNCTION}: Active; triggered by {QUEUE}, Enabled"
    ]


def millis(text: str) -> int:
    return int(datetime.fromisoformat(text).timestamp() * 1000)


def test_logs_are_the_recent_lines_made_short():
    logs = FakeLogs()
    logs.groups[LOG_GROUP] = [
        {"timestamp": millis("2026-11-03 10:00:00"), "message": "an hour too old\n"},
        {
            "timestamp": millis("2026-11-03 11:40:00"),
            "message": "START RequestId: 1 Version: $LATEST\n",
        },
        {
            "timestamp": millis("2026-11-03 11:40:02"),
            "message": "[INFO]\t2026-11-03T09:40:02.123Z\t1\t2019-06: loaded 715,131 trips\n",
        },
        {
            "timestamp": millis("2026-11-03 11:40:01"),
            "message": "[ERROR]\t2026-11-03T09:40:01.000Z\t1\tfailed\rTraceback\r  line 1\n",
        },
    ]
    now = millis("2026-11-03 12:00:00") / 1000

    assert recent_logs(logs, minutes=30, now=now) == [
        "11:40:00 START RequestId: 1 Version: $LATEST",
        "11:40:01 ERROR failed\nTraceback\n  line 1",
        "11:40:02 INFO 2019-06: loaded 715,131 trips",
    ]
    assert recent_logs(FakeLogs(), minutes=30) == []


def test_remove_takes_away_everything_deploy_made(cloud, tmp_path):
    cloud.deploy(make_package(tmp_path))
    cloud.logs.groups[LOG_GROUP] = []
    lines: list[str] = []

    remove(cloud.lambda_client, cloud.iam, cloud.logs, report=lines.append)

    assert (cloud.lambda_client.functions, cloud.lambda_client.mappings) == ({}, {})
    assert (cloud.iam.roles, cloud.logs.groups) == ({}, {})
    assert len(lines) == 4

    remove(cloud.lambda_client, cloud.iam, cloud.logs, report=lines.append)
    assert len(lines) == 4  # nothing left to remove, nothing said
