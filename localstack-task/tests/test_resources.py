from __future__ import annotations

import pytest

from bikes.resources import TABLES, FileKind, S3File, notification_prefixes


def test_keys_follow_the_monthly_hive_layout():
    assert S3File(FileKind.RAW, "2019-06").key == "raw/year=2019/month=06/trips_2019-06.csv"
    assert (
        S3File(FileKind.METRICS, "2019-06").key
        == "metrics/year=2019/month=06/station_counts_2019-06.csv"
    )


@pytest.mark.parametrize("kind", list(FileKind))
def test_a_key_names_its_file(kind):
    file = S3File(kind, "2016-05")

    assert S3File.from_key(file.key) == file


def test_the_partner_is_the_months_other_file():
    raw = S3File(FileKind.RAW, "2020-03")

    assert raw.partner == S3File(FileKind.METRICS, "2020-03")
    assert raw.partner.partner == raw


@pytest.mark.parametrize(
    "key",
    [
        "raw/year=2019/month=06/trips_2019-07.csv",  # month differs from its folder
        "raw/year=2019/month=06/station_counts_2019-06.csv",  # metrics name under raw/
        "processed/year=2019/month=06/trips_2019-06.csv",
        "raw/year=2019/month=06/trips_2019-06.csv.part",
        "raw/2019/06/trips_2019-06.csv",
        "raw/year=1999/month=01/notes.txt",
    ],
)
def test_other_keys_are_not_ours(key):
    assert S3File.from_key(key) is None


def test_both_prefixes_notify():
    assert notification_prefixes() == ["raw/", "metrics/"]


# A few of DynamoDB's reserved words that would make natural key names
RESERVED = {"DATE", "DAY", "MONTH", "YEAR", "NAME", "PART", "COUNT", "STATUS", "TIME", "RETURN"}


@pytest.mark.parametrize("spec", TABLES, ids=lambda spec: spec.name)
def test_table_keys_are_strings_and_not_reserved_words(spec):
    definitions = spec.attribute_definitions()

    assert {d["AttributeType"] for d in definitions} == {"S"}
    assert not {d["AttributeName"].upper() for d in definitions} & RESERVED
    assert spec.key_schema()[0] == {"AttributeName": spec.partition_key, "KeyType": "HASH"}
