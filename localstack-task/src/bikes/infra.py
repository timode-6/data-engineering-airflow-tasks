"""Create, check and remove the pipeline's AWS resources in LocalStack

    python -m bikes.infra up              # creates whatever is missing; safe to repeat
    python -m bikes.infra check           # drops a file into S3, waits for its event on SQS
    python -m bikes.infra deploy          # packages the loader Lambda, has the queue call it
    python -m bikes.infra status          # what exists, and how much is in it
    python -m bikes.infra logs            # the Lambda's log of the last 30 minutes
    python -m bikes.infra loads           # each month's load: what it wrote, how long it took
    python -m bikes.infra reload 2019-06  # loads months again, after a loader change
    python -m bikes.infra redrive         # moves dead letters back to the queue, after a fix
    python -m bikes.infra down            # deletes it all, after asking

`up` creates the bucket with its notifications to the queue, the queue and its
dead-letter queue, and the five tables. `check` works only before `deploy`:
after it, the Lambda takes the queue's messages. LocalStack's free edition
keeps nothing across restarts, so run `up` and `deploy` again after one
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from bikes import aws, deploy
from bikes.config import AwsSettings, ConfigError
from bikes.files import month_argument
from bikes.loader.stores import Ledger, S3Files, utc_now
from bikes.package import PackageError, build_package, lambda_architecture
from bikes.resources import (
    BUCKET,
    DEAD_LETTER_QUEUE,
    LOADER_FUNCTION,
    LOADER_TIMEOUT,
    LOADS,
    QUEUE,
    TABLES,
    FileKind,
    S3File,
    TableSpec,
    notification_prefixes,
)
from bikes.s3_events import event_body, object_keys

# Longer than the Lambda may run, as AWS requires. AWS suggests six times the
# timeout
VISIBILITY_TIMEOUT = LOADER_TIMEOUT + 60
# The Lambda checks for a month's second file every 30 s: 20 receives wait 10 minutes
MAX_RECEIVES = 20
RETENTION = 14 * 24 * 3600  # the longest SQS keeps a message
CHECK_FILE = S3File(FileKind.RAW, "1999-01")  # a month without data, for `check`

Report = Callable[[str], None]


class CheckError(RuntimeError):
    """S3 did not deliver the check file's event to the queue"""


@dataclass(frozen=True)
class Queues:
    url: str
    arn: str
    dead_letter_arn: str


def created_or_exists(created: bool) -> str:
    return "created" if created else "exists"


# S3


def bucket_names(s3: Any) -> set[str]:
    return {bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])}


def ensure_bucket(s3: Any, region: str, bucket: str = BUCKET) -> bool:
    """Create the bucket unless it exists; True if it was created"""
    if bucket in bucket_names(s3):
        return False
    if region == "us-east-1":  # the one region that refuses a location constraint
        s3.create_bucket(Bucket=bucket)
    else:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
    return True


def notification_configuration(queue_arn: str) -> dict[str, Any]:
    """New .csv objects under each prefix go to the queue as events"""
    return {
        "QueueConfigurations": [
            {
                "Id": f"{prefix.rstrip('/')}-files-to-queue",
                "QueueArn": queue_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "prefix", "Value": prefix},
                            {"Name": "suffix", "Value": ".csv"},
                        ]
                    }
                },
            }
            for prefix in notification_prefixes()
        ]
    }


def empty_bucket(s3: Any, bucket: str = BUCKET) -> int:
    """Delete every object in the bucket; returns how many there were"""
    deleted = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        keys = [{"Key": entry["Key"]} for entry in page.get("Contents", [])]
        if keys:  # a page holds at most 1,000, which is delete_objects' limit too
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            deleted += len(keys)
    return deleted


# --- SQS


def queue_url(sqs: Any, name: str) -> str | None:
    """The URL of the queue called exactly name, or None"""
    urls = sqs.list_queues(QueueNamePrefix=name).get("QueueUrls", [])
    return next((url for url in urls if url.rsplit("/", 1)[-1] == name), None)


def ensure_queue(sqs: Any, name: str) -> tuple[str, bool]:
    """The queue's URL, and whether it had to be created"""
    url = queue_url(sqs, name)
    if url is not None:
        return url, False
    return sqs.create_queue(QueueName=name)["QueueUrl"], True


def queue_arn(sqs: Any, url: str) -> str:
    attributes = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return attributes["Attributes"]["QueueArn"]


def queue_policy(queue: str, bucket: str = BUCKET) -> str:
    """Lets the bucket, and only the bucket, send its events to the queue"""
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "BucketEventsOnly",
                    "Effect": "Allow",
                    "Principal": {"Service": "s3.amazonaws.com"},
                    "Action": "sqs:SendMessage",
                    "Resource": queue,
                    "Condition": {"ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{bucket}"}},
                }
            ],
        }
    )


def main_queue_attributes(arn: str, dead_letter_arn: str) -> dict[str, str]:
    redrive = {"deadLetterTargetArn": dead_letter_arn, "maxReceiveCount": str(MAX_RECEIVES)}
    return {
        "VisibilityTimeout": str(VISIBILITY_TIMEOUT),
        "MessageRetentionPeriod": str(RETENTION),
        "RedrivePolicy": json.dumps(redrive),
        "Policy": queue_policy(arn),
    }


def ensure_queues(sqs: Any, report: Report) -> Queues:
    """Both queues, created if missing; their settings are applied either way"""
    dead_letter_url, created = ensure_queue(sqs, DEAD_LETTER_QUEUE)
    sqs.set_queue_attributes(
        QueueUrl=dead_letter_url, Attributes={"MessageRetentionPeriod": str(RETENTION)}
    )
    dead_letter_arn = queue_arn(sqs, dead_letter_url)
    report(f"SQS {DEAD_LETTER_QUEUE}: {created_or_exists(created)}")

    url, created = ensure_queue(sqs, QUEUE)
    arn = queue_arn(sqs, url)
    sqs.set_queue_attributes(QueueUrl=url, Attributes=main_queue_attributes(arn, dead_letter_arn))
    report(
        f"SQS {QUEUE}: {created_or_exists(created)} (visibility {VISIBILITY_TIMEOUT} s, "
        f"to {DEAD_LETTER_QUEUE} after {MAX_RECEIVES} receives)"
    )
    return Queues(url=url, arn=arn, dead_letter_arn=dead_letter_arn)


# --- DynamoDB


def table_names(dynamodb: Any) -> set[str]:
    names: set[str] = set()
    for page in dynamodb.get_paginator("list_tables").paginate():
        names.update(page["TableNames"])
    return names


def ensure_table(dynamodb: Any, spec: TableSpec, existing: set[str]) -> bool:
    """Create the table unless it exists, True if it was created"""
    if spec.name in existing:
        keys = dynamodb.describe_table(TableName=spec.name)["Table"]["KeySchema"]
        if keys != spec.key_schema():
            raise RuntimeError(
                f"{spec.name} exists with keys {keys}, not {spec.key_schema()}; "
                "`python -m bikes.infra down` removes it"
            )
        return False
    dynamodb.create_table(
        TableName=spec.name,
        KeySchema=spec.key_schema(),
        AttributeDefinitions=spec.attribute_definitions(),
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.get_waiter("table_exists").wait(
        TableName=spec.name, WaiterConfig={"Delay": 1, "MaxAttempts": 60}
    )
    return True


# --- commands


def up(s3: Any, sqs: Any, dynamodb: Any, region: str, report: Report = print) -> None:
    """Create every resource that is missing"""
    report(f"S3 bucket {BUCKET}: {created_or_exists(ensure_bucket(s3, region))}")
    queues = ensure_queues(sqs, report)
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET, NotificationConfiguration=notification_configuration(queues.arn)
    )
    prefixes = " and ".join(f"{prefix}*.csv" for prefix in notification_prefixes())
    report(f"S3 → SQS: new {prefixes} notify {QUEUE}")
    existing = table_names(dynamodb)
    for spec in TABLES:
        keys = ", ".join(key["AttributeName"] for key in spec.key_schema())
        created = ensure_table(dynamodb, spec, existing)
        report(f"DynamoDB {spec.name} ({keys}): {created_or_exists(created)}")


def status(s3: Any, sqs: Any, dynamodb: Any, report: Report = print) -> None:
    """What exists, and how much is in it"""
    if BUCKET in bucket_names(s3):
        for prefix in notification_prefixes():
            files = size = 0
            pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
            for page in pages:
                for entry in page.get("Contents", []):
                    files += 1
                    size += entry["Size"]
            report(f"S3 {BUCKET}/{prefix}: {files:,} files, {size / 2**20:,.0f} MiB")
    else:
        report(f"S3 bucket {BUCKET}: missing, run `python -m bikes.infra up`")

    for name in (QUEUE, DEAD_LETTER_QUEUE):
        url = queue_url(sqs, name)
        if url is None:
            report(f"SQS {name}: missing")
            continue
        counts = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        report(
            f"SQS {name}: {counts['ApproximateNumberOfMessages']} waiting, "
            f"{counts['ApproximateNumberOfMessagesNotVisible']} being handled"
        )

    existing = table_names(dynamodb)
    for spec in TABLES:
        if spec.name in existing:
            items = dynamodb.describe_table(TableName=spec.name)["Table"].get("ItemCount", 0)
            report(f"DynamoDB {spec.name}: {items:,} items")
        else:
            report(f"DynamoDB {spec.name}: missing")


def check(s3: Any, sqs: Any, wait_seconds: float = 30) -> float:
    """Seconds between putting CHECK_FILE into S3 and its event reaching the queue"""
    url = queue_url(sqs, QUEUE)
    if url is None:
        raise CheckError(f"queue {QUEUE} does not exist; run `python -m bikes.infra up`")
    key = CHECK_FILE.key
    started = time.monotonic()
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"departure,return\n")
    try:
        while time.monotonic() - started < wait_seconds:
            response = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
            for message in response.get("Messages", []):
                keys = object_keys(message["Body"])
                handle = message["ReceiptHandle"]
                if keys is None or key in keys:
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=handle)
                    if keys is not None:
                        return time.monotonic() - started
                else:
                    sqs.change_message_visibility(
                        QueueUrl=url, ReceiptHandle=handle, VisibilityTimeout=0
                    )
        raise CheckError(f"no event for {key} arrived within {wait_seconds:.0f} s")
    finally:
        s3.delete_object(Bucket=BUCKET, Key=key)


def reload(
    s3: Any,
    sqs: Any,
    ledger: Ledger,
    months: Sequence[str],
    report: Report = print,
    clock: Callable[[], datetime] = utc_now,
) -> int:
    """Have the Lambda load months again; returns how many were queued"""
    files, url = S3Files(s3), queue_url(sqs, QUEUE)
    queued = 0
    for month in months:
        raw = S3File(FileKind.RAW, month)
        missing = [file.key for file in (raw, raw.partner) if files.etag(file.key) is None]
        if missing:
            report(f"{month}: left alone, not in S3: {', '.join(missing)}")
            continue
        if not ledger.forget(month, clock()):
            report(f"{month}: left alone, being loaded right now")
            continue
        sqs.send_message(QueueUrl=url, MessageBody=event_body(raw.key))
        report(f"{month}: queued")
        queued += 1
    return queued


def redrive(sqs: Any, source_url: str, target_url: str) -> int:
    """Move every message from one queue to another, e.g. dead letters back"""
    moved = 0
    while True:
        response = sqs.receive_message(
            QueueUrl=source_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
        )
        messages = response.get("Messages", [])
        if not messages:
            return moved
        entries = [{"Id": str(n), "MessageBody": m["Body"]} for n, m in enumerate(messages)]
        response = sqs.send_message_batch(QueueUrl=target_url, Entries=entries)
        failed = {entry["Id"] for entry in response.get("Failed", [])}
        sent = [
            {"Id": str(n), "ReceiptHandle": m["ReceiptHandle"]}
            for n, m in enumerate(messages)
            if str(n) not in failed
        ]
        if sent:
            sqs.delete_message_batch(QueueUrl=source_url, Entries=sent)
        moved += len(sent)
        if failed:
            raise RuntimeError(
                f"SQS did not take {len(failed)} messages; they stay where they were"
            )


def down(s3: Any, sqs: Any, dynamodb: Any, report: Report = print) -> None:
    """Delete every resource that exists, the bucket's contents included"""
    existing = table_names(dynamodb)
    for spec in TABLES:
        if spec.name in existing:
            dynamodb.delete_table(TableName=spec.name)
            report(f"DynamoDB {spec.name}: deleted")
    for name in (QUEUE, DEAD_LETTER_QUEUE):
        url = queue_url(sqs, name)
        if url is not None:
            sqs.delete_queue(QueueUrl=url)
            report(f"SQS {name}: deleted")
    if BUCKET in bucket_names(s3):
        objects = empty_bucket(s3)
        s3.delete_bucket(Bucket=BUCKET)
        report(f"S3 bucket {BUCKET}: deleted with {objects:,} objects")


def missing_resources(s3: Any, sqs: Any, dynamodb: Any) -> list[str]:
    """What `up` creates and does not exist"""
    missing = [] if BUCKET in bucket_names(s3) else [f"S3 bucket {BUCKET}"]
    missing += [f"SQS {name}" for name in (QUEUE, DEAD_LETTER_QUEUE) if not queue_url(sqs, name)]
    existing = table_names(dynamodb)
    missing += [f"DynamoDB {spec.name}" for spec in TABLES if spec.name not in existing]
    return missing


def local_time(stamp: str) -> str:
    return datetime.fromisoformat(stamp).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def loads_summary(entries: Sequence[dict[str, Any]]) -> str:
    """One line on how many months bike_loads has in each state: loaded, loading, failed"""
    if not entries:
        return "Months: none loaded yet"
    counts = Counter(entry["load_state"] for entry in entries)
    return "Months: " + ", ".join(f"{count} {state}" for state, count in sorted(counts.items()))


def loads_table(entries: Sequence[dict[str, Any]]) -> list[str]:
    """bike_loads as a table, a month per line; a failed load's error under it"""

    def number(entry: dict[str, Any], name: str) -> str:
        return f"{int(entry[name]):,}" if name in entry else ""

    def seconds(entry: dict[str, Any], name: str) -> str:
        return f"{float(entry[name]):.1f}" if name in entry else ""

    if not entries:
        return [loads_summary(entries)]
    lines = [
        f"{'month':<8} {'state':<8} {'trips':>9} {'valid':>9} {'days':>4} {'stations':>8} "
        f"{'raw items':>9} {'seconds':>7} {'S3':>5} {'pandas':>6} {'DynamoDB':>8}  "
        "finished (or started)"
    ]
    for entry in entries:
        when = local_time(entry.get("finished_at") or entry["started_at"])
        lines.append(
            f"{entry[LOADS.partition_key]:<8} {entry['load_state']:<8} "
            f"{number(entry, 'trips'):>9} {number(entry, 'valid_trips'):>9} "
            f"{number(entry, 'days'):>4} {number(entry, 'stations'):>8} "
            f"{number(entry, 'raw_items'):>9} {seconds(entry, 'load_seconds'):>7} "
            f"{seconds(entry, 'read_seconds'):>5} {seconds(entry, 'build_seconds'):>6} "
            f"{seconds(entry, 'write_seconds'):>8}  {when}"
        )
        if "load_error" in entry:
            lines.append(f"{'':<8} {entry['load_error']}")
    lines.append(loads_summary(entries))
    return lines


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m bikes.infra",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("up", help="create whatever is missing")
    commands.add_parser("check", help="test that new S3 files reach the queue")
    commands.add_parser("deploy", help="package the Lambda and make the queue trigger it")
    commands.add_parser("status", help="show what exists and how much is in it")
    logs = commands.add_parser("logs", help="show the Lambda's recent log lines")
    logs.add_argument("--minutes", type=float, default=30, help="how far back (default 30)")
    commands.add_parser("loads", help="show each month's load")
    again = commands.add_parser("reload", help="load months again")
    again.add_argument(
        "months", nargs="*", type=month_argument, metavar="YYYY-MM", help="the months to load"
    )
    again.add_argument("--all", action="store_true", help="every month in bike_loads")
    commands.add_parser("redrive", help="move dead letters back to the queue")
    remove = commands.add_parser("down", help="delete everything")
    remove.add_argument("--yes", action="store_true", help="do not ask first")
    return parser.parse_args(argv)


def run_up(args: argparse.Namespace, settings: AwsSettings) -> None:
    up(aws.client("s3"), aws.client("sqs"), aws.client("dynamodb"), settings.region)


def run_check(args: argparse.Namespace, settings: AwsSettings) -> None:
    if deploy.find_function(aws.client("lambda")) is not None:
        raise SystemExit(
            f"Not checked: {LOADER_FUNCTION} takes the queue's messages now, so the check file's "
            "event would go to it. `python -m bikes.infra logs` shows what it does with each file"
        )
    try:
        seconds = check(aws.client("s3"), aws.client("sqs"))
    except CheckError as error:
        raise SystemExit(f"Check failed: {error}") from error
    print(f"S3 → SQS works: the event for {CHECK_FILE.key} arrived after {seconds:.1f} s")


def run_deploy(args: argparse.Namespace, settings: AwsSettings) -> None:
    s3, sqs = aws.client("s3"), aws.client("sqs")
    missing = missing_resources(s3, sqs, aws.client("dynamodb"))
    if missing:
        raise SystemExit(f"Missing: {', '.join(missing)}. Run `python -m bikes.infra up` first")
    arn = queue_arn(sqs, queue_url(sqs, QUEUE))
    try:
        package = build_package(lambda_architecture())
    except PackageError as error:
        raise SystemExit(f"No package: {error}") from error
    account = aws.client("sts").get_caller_identity()["Account"]
    deploy.deploy(
        aws.client("lambda"),
        aws.client("iam"),
        s3,
        package=package,
        queue_arn=arn,
        policy=deploy.loader_policy(account, settings.region, arn),
    )
    print(
        "The Lambda now takes the queue's messages. Follow it with "
        "`python -m bikes.infra logs` and `python -m bikes.infra loads`"
    )


def run_status(args: argparse.Namespace, settings: AwsSettings) -> None:
    dynamodb = aws.client("dynamodb")
    status(aws.client("s3"), aws.client("sqs"), dynamodb)
    for line in deploy.status_lines(aws.client("lambda")):
        print(line)
    if LOADS.name in table_names(dynamodb):
        print(loads_summary(Ledger(aws.resource("dynamodb").Table(LOADS.name)).entries()))


def run_logs(args: argparse.Namespace, settings: AwsSettings) -> None:
    lines = deploy.recent_logs(aws.client("logs"), args.minutes)
    if not lines:
        print(f"No log lines from {LOADER_FUNCTION} in the last {args.minutes:g} minutes")
    for line in lines:
        print(line)


def run_loads(args: argparse.Namespace, settings: AwsSettings) -> None:
    if LOADS.name not in table_names(aws.client("dynamodb")):
        raise SystemExit(f"{LOADS.name} does not exist; run `python -m bikes.infra up`")
    for line in loads_table(Ledger(aws.resource("dynamodb").Table(LOADS.name)).entries()):
        print(line)


def run_reload(args: argparse.Namespace, settings: AwsSettings) -> None:
    if bool(args.months) == args.all:
        raise SystemExit("Name the months to load again, or pass --all for every loaded month")
    sqs = aws.client("sqs")
    if queue_url(sqs, QUEUE) is None or LOADS.name not in table_names(aws.client("dynamodb")):
        raise SystemExit("The queue or bike_loads does not exist; run `python -m bikes.infra up`")
    ledger = Ledger(aws.resource("dynamodb").Table(LOADS.name))
    months = [entry[LOADS.partition_key] for entry in ledger.entries()] if args.all else args.months
    queued = reload(aws.client("s3"), sqs, ledger, months)
    print(f"{queued} months queued; `python -m bikes.infra logs` shows each load once it is done")


def run_redrive(args: argparse.Namespace, settings: AwsSettings) -> None:
    sqs = aws.client("sqs")
    source, target = queue_url(sqs, DEAD_LETTER_QUEUE), queue_url(sqs, QUEUE)
    if source is None or target is None:
        raise SystemExit("The queues do not exist; run `python -m bikes.infra up` first")
    moved = redrive(sqs, source, target)
    print(f"SQS {DEAD_LETTER_QUEUE} → {QUEUE}: moved {moved:,} messages back")


def run_down(args: argparse.Namespace, settings: AwsSettings) -> None:
    question = (
        "Delete the Lambda, the bucket and its files, both queues and all five tables? [y/N] "
    )
    if not args.yes and input(question).strip().lower() != "y":
        print("Nothing deleted.")
        return
    deploy.remove(aws.client("lambda"), aws.client("iam"), aws.client("logs"))
    down(aws.client("s3"), aws.client("sqs"), aws.client("dynamodb"))


COMMANDS: dict[str, Callable[[argparse.Namespace, AwsSettings], None]] = {
    "up": run_up,
    "check": run_check,
    "deploy": run_deploy,
    "status": run_status,
    "logs": run_logs,
    "loads": run_loads,
    "reload": run_reload,
    "redrive": run_redrive,
    "down": run_down,
}


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        settings = AwsSettings.from_env()
    except ConfigError as error:
        raise SystemExit(str(error)) from error
    COMMANDS[args.command](args, settings)


if __name__ == "__main__":
    main()
