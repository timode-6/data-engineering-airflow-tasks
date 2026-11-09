from __future__ import annotations

import json

from bikes.resources import BUCKET
from bikes.s3_events import event_body, object_keys


def test_keys_are_decoded_as_s3_encodes_them():
    body = event_body("raw/year=2019/month=06/trips_2019-06.csv")

    assert "%3D" in body
    assert object_keys(body) == ["raw/year=2019/month=06/trips_2019-06.csv"]


def test_a_space_sent_as_plus_comes_back_as_a_space():
    assert object_keys(event_body("odd/Kamppi (M).csv")) == ["odd/Kamppi (M).csv"]


def test_the_test_event_names_no_file():
    body = json.dumps({"Service": "Amazon S3", "Event": "s3:TestEvent", "Bucket": "b"})

    assert object_keys(body) is None


def test_every_record_counts():
    first = json.loads(event_body("raw/a.csv"))
    second = json.loads(event_body("metrics/b.csv"))
    body = json.dumps({"Records": first["Records"] + second["Records"]})

    assert object_keys(body) == ["raw/a.csv", "metrics/b.csv"]


def test_an_event_made_here_names_the_pipelines_bucket():
    (record,) = json.loads(event_body("raw/a.csv"))["Records"]

    assert (record["eventSource"], record["s3"]["bucket"]["name"]) == ("aws:s3", BUCKET)
