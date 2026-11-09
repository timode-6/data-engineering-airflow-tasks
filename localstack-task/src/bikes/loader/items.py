"""DynamoDB items for one month (docs/dynamodb.md)"""

from __future__ import annotations

import gzip
import io
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pandas as pd

from bikes.loader.metrics import AVERAGES, DAY, period_labels
from bikes.loader.trips import MonthTrips
from bikes.resources import DAILY_METRICS, MONTHLY_METRICS, STATION_METRICS, TRIPS

MAX_TRIPS_PER_ITEM = 1_000  # about 160 KB of lines, far under DynamoDB's 400 KB item limit
COUNTS_HEADER = ["station_name", "departures", "returns"]

Item = dict[str, Any]


def raw_trip_items(trips: MonthTrips, max_trips: int = MAX_TRIPS_PER_ITEM) -> Iterator[Item]:
    """One item per departure day and station: those trips' lines as a gzipped CSV file"""

    keys = pd.DataFrame(
        {"day": period_labels(trips.frame["departure"], DAY), "station": trips.frame["station"]}
    )
    groups = keys.groupby(["day", "station"], sort=True).indices
    for (day, station), positions in sorted(groups.items()):
        lines = [trips.lines[position] for position in positions]
        for part, start in enumerate(range(0, len(lines), max_trips)):
            chunk = lines[start : start + max_trips]
            body = "\n".join([trips.header, *chunk]) + "\n"
            yield {
                TRIPS.partition_key: day,
                TRIPS.sort_key: f"{station}#{part}",
                "station_name": station,
                "part": part,
                "trip_count": len(chunk),
                "trips_csv_gz": gzip.compress(body.encode(), compresslevel=6, mtime=0),
            }


def figures(row: pd.Series) -> Item:
    """The counts and averages of one period, ready for DynamoDB"""
    item: Item = {
        "trips": int(row["trips"]),
        "valid_trips": int(row["valid_trips"]),
        "temperature_hours": int(row["temperature_hours"]),
    }
    for name in AVERAGES:
        if pd.notna(row[name]):
            item[name] = Decimal(str(round(float(row[name]), 2)))
    return item


def daily_items(daily: pd.DataFrame) -> list[Item]:
    """bike_daily_metrics items from period_metrics(..., DAY)"""
    return [
        {DAILY_METRICS.partition_key: day[:7], DAILY_METRICS.sort_key: day, **figures(row)}
        for day, row in daily.iterrows()
    ]


def monthly_items(monthly: pd.DataFrame, days: pd.Series) -> list[Item]:
    """bike_monthly_metrics items from period_metrics(..., MONTH) and days_per_month"""
    return [
        {
            MONTHLY_METRICS.partition_key: month[:4],
            MONTHLY_METRICS.sort_key: month,
            "days": int(days[month]),
            **figures(row),
        }
        for month, row in monthly.iterrows()
    ]


def station_items(counts_csv: str, month: str) -> list[Item]:
    """bike_station_metrics items from the Spark job's file for month"""
    counts = pd.read_csv(
        io.StringIO(counts_csv), dtype={"station_name": str}, keep_default_na=False
    )
    if list(counts.columns) != COUNTS_HEADER:
        raise ValueError(f"unexpected header {list(counts.columns)} in the station counts")
    return [
        {
            STATION_METRICS.partition_key: month,
            STATION_METRICS.sort_key: station,
            "departures": int(departures),
            "returns": int(returns),
        }
        for station, departures, returns in counts.itertuples(index=False)
    ]
