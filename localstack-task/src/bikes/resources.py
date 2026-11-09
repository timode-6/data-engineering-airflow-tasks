"""The pipeline's AWS resources and S3 keys, shared by the DAG, the Lambda and the scripts
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

BUCKET = "helsinki-city-bikes"
QUEUE = "bike-files"
DEAD_LETTER_QUEUE = "bike-files-dlq"

LOADER_FUNCTION = "bike-loader"
LOADER_ROLE = "bike-loader"
# The Lambda's timeout
LOADER_TIMEOUT = 300
# Outside raw/ and metrics/, so uploading the package sends no event
LOADER_PACKAGE_KEY = f"deploy/{LOADER_FUNCTION}.zip"


class FileKind(StrEnum):
    """The two files every month gets: the trips, and Spark's counts per station"""

    RAW = "raw"
    METRICS = "metrics"

    @property
    def stem(self) -> str:
        return "trips" if self is FileKind.RAW else "station_counts"

    @property
    def partner(self) -> FileKind:
        return FileKind.METRICS if self is FileKind.RAW else FileKind.RAW


KEY = re.compile(
    r"(?P<kind>raw|metrics)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/"
    r"(?P<stem>trips|station_counts)_(?P=year)-(?P=month)\.csv"
)


@dataclass(frozen=True)
class S3File:
    """One of a month's two files in S3"""

    kind: FileKind
    month: str  # "2019-06"

    @property
    def key(self) -> str:
        year, number = self.month.split("-")
        return f"{self.kind}/year={year}/month={number}/{self.kind.stem}_{self.month}.csv"

    @property
    def partner(self) -> S3File:
        """The month's other file, which the Lambda waits for"""
        return S3File(self.kind.partner, self.month)

    @classmethod
    def from_key(cls, key: str) -> S3File | None:
        """The file a key names, or None for anything that is not one of ours"""
        match = KEY.fullmatch(key)
        if match is None or match["stem"] != FileKind(match["kind"]).stem:
            return None
        return cls(FileKind(match["kind"]), f"{match['year']}-{match['month']}")


def notification_prefixes() -> list[str]:
    """The S3 prefixes whose new objects are sent to the queue"""
    return [f"{kind}/" for kind in FileKind]


@dataclass(frozen=True)
class TableSpec:
    """A DynamoDB table: its name, its string keys, and what one item holds"""

    name: str
    partition_key: str
    sort_key: str | None
    item: str

    def key_schema(self) -> list[dict[str, str]]:
        schema = [{"AttributeName": self.partition_key, "KeyType": "HASH"}]
        if self.sort_key:
            schema.append({"AttributeName": self.sort_key, "KeyType": "RANGE"})
        return schema

    def attribute_definitions(self) -> list[dict[str, str]]:
        return [
            {"AttributeName": key["AttributeName"], "AttributeType": "S"}
            for key in self.key_schema()
        ]


TRIPS = TableSpec(
    "bike_trips",
    partition_key="trip_date",  # 2019-06-06
    sort_key="station_part",  # Kamppi (M)#0
    item="the raw trips that left one station on one day, gzip-compressed as their CSV lines",
)
DAILY_METRICS = TableSpec(
    "bike_daily_metrics",
    partition_key="trip_month",  # 2019-06
    sort_key="trip_date",  # 2019-06-06
    item="one day's trip count and averages: distance, duration, speed, temperature",
)
MONTHLY_METRICS = TableSpec(
    "bike_monthly_metrics",
    partition_key="trip_year",  # 2019
    sort_key="trip_month",  # 2019-06
    item="the same for a whole month, computed from its trips, not from the daily averages",
)
STATION_METRICS = TableSpec(
    "bike_station_metrics",
    partition_key="trip_month",
    sort_key="station_name",
    item="departures from and returns to one station in one month, from the Spark file",
)
LOADS = TableSpec(
    "bike_loads",
    partition_key="trip_month",
    sort_key=None,
    item="which pair of files a month was loaded from, or is being loaded from right now",
)
TABLES = (TRIPS, DAILY_METRICS, MONTHLY_METRICS, STATION_METRICS, LOADS)
