"""The loader Lambda in AWS: its IAM role, the function, and the SQS trigger"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from bikes.package import Package
from bikes.resources import (
    BUCKET,
    LOADER_FUNCTION,
    LOADER_PACKAGE_KEY,
    LOADER_ROLE,
    LOADER_TIMEOUT,
    TABLES,
    notification_prefixes,
)

RUNTIME = "python3.12"
HANDLER = "bikes.loader.handler.handle"
DESCRIPTION = "Loads a month's trips and station counts from S3 into DynamoDB"
# pandas peaks at about 1 GB on the biggest month; on AWS, more memory also means more CPU
MEMORY_MB = 2048
BATCH_SIZE = 1  # one S3 event per call, so each message is done, waits or fails on its own
MAX_CONCURRENCY = 2  # the fewest SQS allows; each call needs about 1 GB
LOG_GROUP = f"/aws/lambda/{LOADER_FUNCTION}"
POLICY_NAME = "read-month-files-write-tables"
WAITER_CONFIG = {"Delay": 2, "MaxAttempts": 150}  # up to 5 minutes
TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}
# Lambda's own lines in the Python runtime's log format: "[INFO]\t<time>\t<request id>\t<text>"
LOG_LINE = re.compile(r"\[(?P<level>[A-Z]+)\]\t\S+\t\S+\t(?P<text>.*)", re.DOTALL)

Report = Callable[[str], None]


# IAM


def loader_policy(account: str, region: str, queue_arn: str) -> dict[str, Any]:
    """What the function may do: read the month files and the queue, write the tables, log"""
    prefixes = [f"{prefix}*" for prefix in notification_prefixes()]
    tables = [f"arn:aws:dynamodb:{region}:{account}:table/{spec.name}" for spec in TABLES]
    log_group = f"arn:aws:logs:{region}:{account}:log-group:{LOG_GROUP}"
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadMonthFiles",
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": [f"arn:aws:s3:::{BUCKET}/{prefix}" for prefix in prefixes],
            },
            {
                "Sid": "ListMonthFiles",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{BUCKET}",
                "Condition": {"StringLike": {"s3:prefix": prefixes}},
            },
            {
                "Sid": "TakeMessages",
                "Effect": "Allow",
                "Action": [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:ChangeMessageVisibility",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                ],
                "Resource": queue_arn,
            },
            {
                "Sid": "WriteTables",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:Query",
                    "dynamodb:BatchWriteItem",
                ],
                "Resource": tables,
            },
            {
                "Sid": "WriteLogs",
                "Effect": "Allow",
                "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": [log_group, f"{log_group}:*"],
            },
        ],
    }


def role_arn(iam: Any) -> str | None:
    """The loader role's ARN, or None if it does not exist"""
    for page in iam.get_paginator("list_roles").paginate():
        for role in page.get("Roles", []):
            if role["RoleName"] == LOADER_ROLE:
                return role["Arn"]
    return None


def ensure_role(iam: Any, policy: dict[str, Any]) -> tuple[str, bool]:
    """The role's ARN, and whether it had to be created; its policy is set either way"""
    arn = role_arn(iam)
    created = arn is None
    if created:
        response = iam.create_role(
            RoleName=LOADER_ROLE,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
            Description=f"Runs the {LOADER_FUNCTION} Lambda",
        )
        arn = response["Role"]["Arn"]
    iam.put_role_policy(
        RoleName=LOADER_ROLE, PolicyName=POLICY_NAME, PolicyDocument=json.dumps(policy)
    )
    return arn, created


# Lambda


def find_function(lambda_client: Any) -> dict[str, Any] | None:
    """The function's configuration as ListFunctions gives it, or None if it does not exist"""
    for page in lambda_client.get_paginator("list_functions").paginate():
        for function in page.get("Functions", []):
            if function["FunctionName"] == LOADER_FUNCTION:
                return function
    return None


def wait(lambda_client: Any, waiter: str) -> None:
    lambda_client.get_waiter(waiter).wait(FunctionName=LOADER_FUNCTION, WaiterConfig=WAITER_CONFIG)


def ensure_function(lambda_client: Any, s3: Any, package: Package, role: str) -> str:
    """Create the function, or update it; says which it did"""
    settings = {
        "Role": role,
        "Handler": HANDLER,
        "Runtime": RUNTIME,
        "Timeout": LOADER_TIMEOUT,
        "MemorySize": MEMORY_MB,
        "Description": DESCRIPTION,
    }
    current = find_function(lambda_client)
    if current is None:
        s3.upload_file(str(package.path), BUCKET, LOADER_PACKAGE_KEY)
        lambda_client.create_function(
            FunctionName=LOADER_FUNCTION,
            Code={"S3Bucket": BUCKET, "S3Key": LOADER_PACKAGE_KEY},
            Architectures=[package.architecture],
            **settings,
        )
        wait(lambda_client, "function_active_v2")
        return "created"

    same_code = current.get("CodeSha256") == package.sha256
    if not same_code or current.get("Architectures") != [package.architecture]:
        s3.upload_file(str(package.path), BUCKET, LOADER_PACKAGE_KEY)
        lambda_client.update_function_code(
            FunctionName=LOADER_FUNCTION,
            S3Bucket=BUCKET,
            S3Key=LOADER_PACKAGE_KEY,
            Architectures=[package.architecture],
        )
        wait(lambda_client, "function_updated_v2")
    lambda_client.update_function_configuration(FunctionName=LOADER_FUNCTION, **settings)
    wait(lambda_client, "function_updated_v2")
    return "updated, same code" if same_code else "updated with the new code"


def find_trigger(lambda_client: Any, queue_arn: str | None = None) -> dict[str, Any] | None:
    """The function's event source mapping, from queue_arn if given, or None"""
    response = lambda_client.list_event_source_mappings(FunctionName=LOADER_FUNCTION)
    mappings = response.get("EventSourceMappings", [])
    return next((m for m in mappings if queue_arn in (None, m["EventSourceArn"])), None)


def ensure_trigger(
    lambda_client: Any,
    queue_arn: str,
    wait_seconds: float = 60,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[str, str]:
    """Have the queue trigger the function; returns what was done and the mapping's state"""
    settings = {
        "BatchSize": BATCH_SIZE,
        "FunctionResponseTypes": ["ReportBatchItemFailures"],
        "ScalingConfig": {"MaximumConcurrency": MAX_CONCURRENCY},
        "Enabled": True,
    }
    mapping = find_trigger(lambda_client, queue_arn)
    if mapping is None:
        mapping = lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn, FunctionName=LOADER_FUNCTION, **settings
        )
        done = "created"
    else:
        mapping = lambda_client.update_event_source_mapping(
            UUID=mapping["UUID"], FunctionName=LOADER_FUNCTION, **settings
        )
        done = "updated"
    deadline = time.monotonic() + wait_seconds
    state = mapping.get("State", "")
    while state != "Enabled" and time.monotonic() < deadline:
        sleep(1)
        state = lambda_client.get_event_source_mapping(UUID=mapping["UUID"])["State"]
    return done, state


def deploy(
    lambda_client: Any,
    iam: Any,
    s3: Any,
    *,
    package: Package,
    queue_arn: str,
    policy: dict[str, Any],
    report: Report = print,
) -> None:
    """Role, function and trigger, each created or updated"""
    role, created = ensure_role(iam, policy)
    report(f"IAM role {LOADER_ROLE}: {'created' if created else 'exists'}, policy set")
    done = ensure_function(lambda_client, s3, package, role)
    report(
        f"Lambda {LOADER_FUNCTION}: {done} ({RUNTIME}, {package.architecture}, "
        f"{MEMORY_MB} MB, {LOADER_TIMEOUT} s timeout)"
    )
    done, state = ensure_trigger(lambda_client, queue_arn)
    queue = queue_arn.rsplit(":", 1)[-1]
    report(
        f"SQS {queue} → Lambda {LOADER_FUNCTION}: {done}, {state} "
        f"({BATCH_SIZE} message per call, at most {MAX_CONCURRENCY} calls at once)"
    )


# status, logs, down


def status_lines(lambda_client: Any) -> list[str]:
    """The function's state and its trigger's, for `status`"""
    if find_function(lambda_client) is None:
        return [f"Lambda {LOADER_FUNCTION}: not deployed, run `python -m bikes.infra deploy`"]
    config = lambda_client.get_function_configuration(FunctionName=LOADER_FUNCTION)
    state = config.get("State", "state unknown")
    if config.get("StateReason"):
        state += f" ({config['StateReason']})"
    trigger = find_trigger(lambda_client)
    if trigger is None:
        reads = "no trigger"
    else:
        reads = f"triggered by {trigger['EventSourceArn'].rsplit(':', 1)[-1]}, {trigger['State']}"
    return [f"Lambda {LOADER_FUNCTION}: {state}; {reads}"]


def log_group_exists(logs: Any) -> bool:
    groups = logs.describe_log_groups(logGroupNamePrefix=LOG_GROUP).get("logGroups", [])
    return any(group["logGroupName"] == LOG_GROUP for group in groups)


def format_log_event(event: dict[str, Any]) -> str:
    """One log event as "16:47:12 INFO text", in local time."""
    when = datetime.fromtimestamp(event["timestamp"] / 1000).strftime("%H:%M:%S")
    text = event["message"].replace("\r", "\n").rstrip()
    match = LOG_LINE.fullmatch(text)
    if match:
        text = f"{match['level']} {match['text']}"
    return f"{when} {text}"


def recent_logs(logs: Any, minutes: float, now: float | None = None) -> list[str]:
    """The function's log lines of the last minutes, oldest first"""
    if not log_group_exists(logs):
        return []
    start = int(((now if now is not None else time.time()) - minutes * 60) * 1000)
    pages = logs.get_paginator("filter_log_events").paginate(
        logGroupName=LOG_GROUP, startTime=start
    )
    events = [event for page in pages for event in page.get("events", [])]
    return [format_log_event(event) for event in sorted(events, key=lambda e: e["timestamp"])]


def remove(lambda_client: Any, iam: Any, logs: Any, report: Report = print) -> None:
    """Delete the trigger, the function, its role and its logs, whichever exist"""
    if find_function(lambda_client) is not None:
        response = lambda_client.list_event_source_mappings(FunctionName=LOADER_FUNCTION)
        for mapping in response.get("EventSourceMappings", []):
            lambda_client.delete_event_source_mapping(UUID=mapping["UUID"])
            report(f"Lambda trigger from {mapping['EventSourceArn'].rsplit(':', 1)[-1]}: deleted")
        lambda_client.delete_function(FunctionName=LOADER_FUNCTION)
        report(f"Lambda {LOADER_FUNCTION}: deleted")
    if role_arn(iam) is not None:
        for name in iam.list_role_policies(RoleName=LOADER_ROLE).get("PolicyNames", []):
            iam.delete_role_policy(RoleName=LOADER_ROLE, PolicyName=name)
        iam.delete_role(RoleName=LOADER_ROLE)
        report(f"IAM role {LOADER_ROLE}: deleted")
    if log_group_exists(logs):
        logs.delete_log_group(logGroupName=LOG_GROUP)
        report(f"Logs {LOG_GROUP}: deleted")
