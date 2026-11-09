"""In-memory stand-ins for the boto3 clients and resources the code uses"""

from __future__ import annotations

import base64
import hashlib
import io
import itertools
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from bikes.resources import TableSpec
from bikes.s3_events import event_body

ACCOUNT = "000000000000"
REGION = "eu-north-1"


def client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class Waiter:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def wait(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


class Pages:
    def __init__(self, pages: list[dict]) -> None:
        self._pages = pages

    def paginate(self, **kwargs: Any) -> list[dict]:
        return self._pages


# -- SQS


class FakeSQS:
    """Queues of message bodies"""

    def __init__(self) -> None:
        self.queues: dict[str, dict[str, Any]] = {}
        self._handles = itertools.count()

    def url(self, name: str) -> str:
        return f"http://localhost:4566/{ACCOUNT}/{name}"

    def arn(self, name: str) -> str:
        return f"arn:aws:sqs:{REGION}:{ACCOUNT}:{name}"

    def list_queues(self, **kwargs: Any) -> dict:
        prefix = kwargs.get("QueueNamePrefix", "")
        urls = [url for url in self.queues if url.rsplit("/", 1)[-1].startswith(prefix)]
        return {"QueueUrls": urls} if urls else {}

    def create_queue(self, **kwargs: Any) -> dict:
        url = self.url(kwargs["QueueName"])
        self.queues[url] = {"attributes": {}, "messages": [], "in_flight": {}, "delayed": []}
        return {"QueueUrl": url}

    def get_queue_url(self, **kwargs: Any) -> dict:
        url = self.url(kwargs["QueueName"])
        if url not in self.queues or kwargs.get("QueueOwnerAWSAccountId", ACCOUNT) != ACCOUNT:
            raise client_error("AWS.SimpleQueueService.NonExistentQueue", "GetQueueUrl")
        return {"QueueUrl": url}

    def set_queue_attributes(self, **kwargs: Any) -> None:
        self.queues[kwargs["QueueUrl"]]["attributes"].update(kwargs["Attributes"])

    def get_queue_attributes(self, **kwargs: Any) -> dict:
        queue = self.queues[kwargs["QueueUrl"]]
        name = kwargs["QueueUrl"].rsplit("/", 1)[-1]
        return {
            "Attributes": {
                "QueueArn": self.arn(name),
                "ApproximateNumberOfMessages": str(len(queue["messages"])),
                "ApproximateNumberOfMessagesNotVisible": str(
                    len(queue["in_flight"]) + len(queue["delayed"])
                ),
            }
        }

    def send(self, name: str, body: str) -> None:
        self.queues[self.url(name)]["messages"].append(body)

    def send_message(self, **kwargs: Any) -> None:
        self.queues[kwargs["QueueUrl"]]["messages"].append(kwargs["MessageBody"])

    def receive_message(self, **kwargs: Any) -> dict:
        queue = self.queues[kwargs["QueueUrl"]]
        taken = queue["messages"][: kwargs.get("MaxNumberOfMessages", 1)]
        del queue["messages"][: len(taken)]
        messages = []
        for body in taken:
            handle = str(next(self._handles))
            queue["in_flight"][handle] = body
            messages.append(
                {"MessageId": f"message-{handle}", "Body": body, "ReceiptHandle": handle}
            )
        return {"Messages": messages} if messages else {}

    def delete_message(self, **kwargs: Any) -> None:
        del self.queues[kwargs["QueueUrl"]]["in_flight"][kwargs["ReceiptHandle"]]

    def send_message_batch(self, **kwargs: Any) -> dict:
        queue = self.queues[kwargs["QueueUrl"]]
        queue["messages"].extend(entry["MessageBody"] for entry in kwargs["Entries"])
        return {"Successful": [{"Id": entry["Id"]} for entry in kwargs["Entries"]]}

    def delete_message_batch(self, **kwargs: Any) -> None:
        for entry in kwargs["Entries"]:
            del self.queues[kwargs["QueueUrl"]]["in_flight"][entry["ReceiptHandle"]]

    def change_message_visibility(self, **kwargs: Any) -> None:
        queue = self.queues[kwargs["QueueUrl"]]
        body = queue["in_flight"].pop(kwargs["ReceiptHandle"])
        if kwargs["VisibilityTimeout"] == 0:
            queue["messages"].append(body)
        else:
            queue["delayed"].append((body, kwargs["VisibilityTimeout"]))

    def release_delayed(self, name: str) -> list[int]:
        """Make delayed messages visible again; returns the timeouts they had."""
        queue = self.queues[self.url(name)]
        timeouts = [seconds for _, seconds in queue["delayed"]]
        queue["messages"].extend(body for body, _ in queue["delayed"])
        queue["delayed"].clear()
        return timeouts

    def delete_queue(self, **kwargs: Any) -> None:
        del self.queues[kwargs["QueueUrl"]]


# --- S3


def etag_of(body: bytes) -> str:
    return f'"{hashlib.md5(body).hexdigest()}"'


class FakeS3:
    def __init__(self, notify: FakeSQS | None = None, queue: str = "bike-files") -> None:
        self.buckets: dict[str, dict[str, bytes]] = {}
        self.create_requests: list[dict] = []
        self.notifications: dict | None = None
        self.uploads: list[str] = []
        self._notify = notify
        self._queue = queue

    def list_buckets(self) -> dict:
        return {"Buckets": [{"Name": name} for name in self.buckets]}

    def create_bucket(self, **kwargs: Any) -> None:
        self.buckets[kwargs["Bucket"]] = {}
        self.create_requests.append(kwargs)

    def put_bucket_notification_configuration(self, **kwargs: Any) -> None:
        self.notifications = kwargs["NotificationConfiguration"]

    def put_object(self, **kwargs: Any) -> None:
        body = kwargs["Body"]
        self.buckets[kwargs["Bucket"]][kwargs["Key"]] = (
            body.encode() if isinstance(body, str) else body
        )
        if self._notify is not None:
            self._notify.send(self._queue, event_body(kwargs["Key"]))

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:  # noqa: N803
        self.buckets[Bucket][Key] = Path(Filename).read_bytes()
        self.uploads.append(Key)

    def get_object(self, **kwargs: Any) -> dict:
        body = self.buckets[kwargs["Bucket"]].get(kwargs["Key"])
        if body is None:
            raise client_error("NoSuchKey", "GetObject")
        if "IfMatch" in kwargs and kwargs["IfMatch"] != etag_of(body):
            raise client_error("PreconditionFailed", "GetObject")
        return {"Body": io.BytesIO(body), "ETag": etag_of(body)}

    def delete_object(self, **kwargs: Any) -> None:
        self.buckets[kwargs["Bucket"]].pop(kwargs["Key"], None)

    def list_objects_v2(self, **kwargs: Any) -> dict:
        objects = self.buckets[kwargs["Bucket"]]
        prefix = kwargs.get("Prefix", "")
        keys = sorted(key for key in objects if key.startswith(prefix))
        contents = [
            {"Key": key, "Size": len(objects[key]), "ETag": etag_of(objects[key])}
            for key in keys[: kwargs.get("MaxKeys", 1000)]
        ]
        return {"Contents": contents} if contents else {}

    def get_paginator(self, name: str) -> Any:
        assert name == "list_objects_v2"
        listing = self.list_objects_v2

        class Listing:
            def paginate(self, **kwargs: Any) -> list[dict]:
                return [listing(**kwargs, MaxKeys=10**9)]

        return Listing()

    def delete_objects(self, **kwargs: Any) -> None:
        for entry in kwargs["Delete"]["Objects"]:
            del self.buckets[kwargs["Bucket"]][entry["Key"]]

    def delete_bucket(self, **kwargs: Any) -> None:
        assert not self.buckets[kwargs["Bucket"]], "S3 refuses to delete a bucket with files"
        del self.buckets[kwargs["Bucket"]]


# --- DynamoDB


def holds(condition: Any, item: dict | None) -> bool:
    """Evaluate a boto3 condition (Attr or Key built) against an item, or no item"""
    expression = condition.get_expression()
    operator, values = expression["operator"], expression["values"]
    if operator == "AND":
        return holds(values[0], item) and holds(values[1], item)
    if operator == "OR":
        return holds(values[0], item) or holds(values[1], item)
    if operator == "NOT":
        return not holds(values[0], item)
    attributes = item or {}
    name = values[0].name
    if operator == "attribute_exists":
        return name in attributes
    if operator == "attribute_not_exists":
        return name not in attributes
    if name not in attributes:
        return False  # any comparison with a missing attribute is false
    value, other = attributes[name], values[1]
    comparisons = {
        "=": value == other,
        "<>": value != other,
        "<": value < other,
        "<=": value <= other,
        ">": value > other,
        ">=": value >= other,
        "begins_with": str(value).startswith(other),
    }
    return comparisons[operator]


class FakeBatchWriter:
    def __init__(self, table: FakeTable) -> None:
        self._table = table

    def __enter__(self) -> FakeBatchWriter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        pass

    def put_item(self, Item: dict) -> None:  # noqa: N803
        with self._table.lock:
            self._table.items[self._table.key_of(Item)] = dict(Item)
            self._table.writes += 1

    def delete_item(self, Key: dict) -> None:  # noqa: N803
        with self._table.lock:
            self._table.items.pop(self._table.key_of(Key), None)
            self._table.deletes += 1


class FakeTable:
    """A table resource, safe to use from several threads"""

    def __init__(self, spec: TableSpec, page_size: int = 1000) -> None:
        self.name = spec.name
        self.key_names = [name for name in (spec.partition_key, spec.sort_key) if name]
        self.items: dict[tuple, dict] = {}
        self.page_size = page_size
        self.writes = self.deletes = 0
        self.lock = threading.RLock()

    def key_of(self, item: dict) -> tuple:
        return tuple(item[name] for name in self.key_names)

    def put_item(self, Item: dict, ConditionExpression: Any = None) -> None:  # noqa: N803
        with self.lock:
            current = self.items.get(self.key_of(Item))
            if ConditionExpression is not None and not holds(ConditionExpression, current):
                raise client_error("ConditionalCheckFailedException", "PutItem")
            self.items[self.key_of(Item)] = dict(Item)

    def get_item(self, Key: dict, ConsistentRead: bool = False) -> dict:  # noqa: N803
        with self.lock:
            item = self.items.get(self.key_of(Key))
        return {"Item": dict(item)} if item is not None else {}

    def delete_item(self, Key: dict, ConditionExpression: Any = None) -> None:  # noqa: N803
        with self.lock:
            current = self.items.get(self.key_of(Key))
            if ConditionExpression is not None and not holds(ConditionExpression, current):
                raise client_error("ConditionalCheckFailedException", "DeleteItem")
            self.items.pop(self.key_of(Key), None)

    def _pages(self, items: list[dict], start: dict | None) -> dict:
        ordered = sorted(items, key=self.key_of)
        if start is not None:
            ordered = [item for item in ordered if self.key_of(item) > self.key_of(start)]
        page = ordered[: self.page_size]
        response: dict[str, Any] = {"Items": [dict(item) for item in page]}
        if len(ordered) > self.page_size:
            response["LastEvaluatedKey"] = {name: page[-1][name] for name in self.key_names}
        return response

    def query(self, KeyConditionExpression: Any, **kwargs: Any) -> dict:  # noqa: N803
        with self.lock:
            items = list(self.items.values())
        matching = [item for item in items if holds(KeyConditionExpression, item)]
        response = self._pages(matching, kwargs.get("ExclusiveStartKey"))
        if "ProjectionExpression" in kwargs:
            names = [name.strip() for name in kwargs["ProjectionExpression"].split(",")]
            response["Items"] = [{name: item[name] for name in names} for item in response["Items"]]
        return response

    def scan(self, **kwargs: Any) -> dict:
        with self.lock:
            items = list(self.items.values())
        return self._pages(items, kwargs.get("ExclusiveStartKey"))

    def batch_writer(self) -> FakeBatchWriter:
        return FakeBatchWriter(self)


class FakeDynamoDB:
    """The DynamoDB service resource, holding a FakeTable per spec"""

    def __init__(self, specs: list[TableSpec], page_size: int = 1000) -> None:
        self.tables = {spec.name: FakeTable(spec, page_size) for spec in specs}

    def Table(self, name: str) -> FakeTable:  # noqa: N802
        return self.tables[name]

    def items(self, spec: TableSpec) -> Iterator[dict]:
        return iter(self.tables[spec.name].items.values())


class FakeTables:
    """The DynamoDB client, as far as bikes.infra uses it"""

    def __init__(self) -> None:
        self.tables: dict[str, list[dict]] = {}
        self.created: list[dict] = []

    def get_paginator(self, name: str) -> Pages:
        assert name == "list_tables"
        return Pages([{"TableNames": list(self.tables)}])

    def describe_table(self, **kwargs: Any) -> dict:
        return {"Table": {"KeySchema": self.tables[kwargs["TableName"]], "ItemCount": 0}}

    def create_table(self, **kwargs: Any) -> None:
        self.tables[kwargs["TableName"]] = kwargs["KeySchema"]
        self.created.append(kwargs)

    def get_waiter(self, name: str) -> Waiter:
        return Waiter()

    def delete_table(self, **kwargs: Any) -> None:
        del self.tables[kwargs["TableName"]]


# --- Lambda, IAM, logs, STS

class FakeLambda:
    """Functions and event source mappings"""

    def __init__(self, s3: FakeS3, new_mapping_state: str = "Enabled") -> None:
        self.functions: dict[str, dict] = {}
        self.mappings: dict[str, dict] = {}
        self.calls: list[str] = []
        self.waited: list[str] = []
        self.new_mapping_state = new_mapping_state
        self._s3 = s3
        self._uuids = itertools.count(1)

    def _code_sha256(self, bucket: str, key: str) -> str:
        return base64.b64encode(hashlib.sha256(self._s3.buckets[bucket][key]).digest()).decode()

    def get_paginator(self, name: str) -> Pages:
        assert name == "list_functions"
        listing = [{k: v for k, v in f.items() if k != "State"} for f in self.functions.values()]
        return Pages([{"Functions": listing}])

    def create_function(self, **kwargs: Any) -> dict:
        self.calls.append("create_function")
        code = kwargs.pop("Code")
        self.functions[kwargs["FunctionName"]] = {
            **kwargs,
            "CodeSha256": self._code_sha256(code["S3Bucket"], code["S3Key"]),
            "State": "Active",
        }
        return self.functions[kwargs["FunctionName"]]

    def update_function_code(self, **kwargs: Any) -> None:
        self.calls.append("update_function_code")
        function = self.functions[kwargs["FunctionName"]]
        function["CodeSha256"] = self._code_sha256(kwargs["S3Bucket"], kwargs["S3Key"])
        function["Architectures"] = kwargs["Architectures"]

    def update_function_configuration(self, **kwargs: Any) -> None:
        self.calls.append("update_function_configuration")
        self.functions[kwargs["FunctionName"]].update(kwargs)

    def get_function_configuration(self, **kwargs: Any) -> dict:
        return dict(self.functions[kwargs["FunctionName"]])

    def get_waiter(self, name: str) -> Waiter:
        self.waited.append(name)
        return Waiter()

    def delete_function(self, **kwargs: Any) -> None:
        del self.functions[kwargs["FunctionName"]]

    def list_event_source_mappings(self, **kwargs: Any) -> dict:
        found = [m for m in self.mappings.values() if m["FunctionName"] == kwargs["FunctionName"]]
        return {"EventSourceMappings": [dict(m) for m in found]}

    def create_event_source_mapping(self, **kwargs: Any) -> dict:
        self.calls.append("create_event_source_mapping")
        uuid = f"mapping-{next(self._uuids)}"
        self.mappings[uuid] = {**kwargs, "UUID": uuid, "State": self.new_mapping_state}
        return dict(self.mappings[uuid])

    def update_event_source_mapping(self, **kwargs: Any) -> dict:
        self.calls.append("update_event_source_mapping")
        mapping = self.mappings[kwargs["UUID"]]
        mapping.update(kwargs, State=self.new_mapping_state)
        return dict(mapping)

    def get_event_source_mapping(self, **kwargs: Any) -> dict:
        mapping = self.mappings[kwargs["UUID"]]
        mapping["State"] = "Enabled"
        return dict(mapping)

    def delete_event_source_mapping(self, **kwargs: Any) -> None:
        del self.mappings[kwargs["UUID"]]


class FakeIAM:
    def __init__(self) -> None:
        self.roles: dict[str, dict] = {}

    def get_paginator(self, name: str) -> Pages:
        assert name == "list_roles"
        roles = [{"RoleName": name, "Arn": role["Arn"]} for name, role in self.roles.items()]
        return Pages([{"Roles": roles}])

    def create_role(self, **kwargs: Any) -> dict:
        arn = f"arn:aws:iam::{ACCOUNT}:role/{kwargs['RoleName']}"
        self.roles[kwargs["RoleName"]] = {**kwargs, "Arn": arn, "policies": {}}
        return {"Role": {"RoleName": kwargs["RoleName"], "Arn": arn}}

    def put_role_policy(self, **kwargs: Any) -> None:
        self.roles[kwargs["RoleName"]]["policies"][kwargs["PolicyName"]] = kwargs["PolicyDocument"]

    def list_role_policies(self, **kwargs: Any) -> dict:
        return {"PolicyNames": list(self.roles[kwargs["RoleName"]]["policies"])}

    def delete_role_policy(self, **kwargs: Any) -> None:
        del self.roles[kwargs["RoleName"]]["policies"][kwargs["PolicyName"]]

    def delete_role(self, **kwargs: Any) -> None:
        assert not self.roles[kwargs["RoleName"]]["policies"], "IAM refuses: DeleteConflict"
        del self.roles[kwargs["RoleName"]]


class FakeLogs:
    def __init__(self) -> None:
        self.groups: dict[str, list[dict]] = {}

    def describe_log_groups(self, **kwargs: Any) -> dict:
        prefix = kwargs.get("logGroupNamePrefix", "")
        names = [name for name in self.groups if name.startswith(prefix)]
        return {"logGroups": [{"logGroupName": name} for name in names]}

    def get_paginator(self, name: str) -> Any:
        assert name == "filter_log_events"
        groups = self.groups

        class Filter:
            def paginate(self, **kwargs: Any) -> list[dict]:
                events = groups[kwargs["logGroupName"]]
                start = kwargs.get("startTime", 0)
                return [{"events": [event for event in events if event["timestamp"] >= start]}]

        return Filter()

    def delete_log_group(self, **kwargs: Any) -> None:
        del self.groups[kwargs["logGroupName"]]


class FakeSTS:
    def get_caller_identity(self) -> dict:
        return {"Account": ACCOUNT}
