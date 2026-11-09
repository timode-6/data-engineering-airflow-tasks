"""Everything one month puts into DynamoDB, made from the text of its two files, without AWS"""

from __future__ import annotations

import calendar
from dataclasses import dataclass

from bikes.loader.items import Item, daily_items, monthly_items, raw_trip_items, station_items
from bikes.loader.metrics import DAY, MONTH, days_per_month, period_metrics, with_validity
from bikes.loader.trips import parse_trips
from bikes.resources import DAILY_METRICS, MONTHLY_METRICS, STATION_METRICS, TRIPS, TableSpec


@dataclass(frozen=True)
class TableSlice:
    """A month's items for one table, and the partitions that hold this month alone"""

    spec: TableSpec
    items: list[Item]
    partitions: tuple[str, ...]


@dataclass(frozen=True)
class MonthItems:
    """One month's items, table by table, and the counts that describe them"""

    month: str
    raw_trips: list[Item]
    daily: list[Item]
    monthly: list[Item]
    stations: list[Item]
    trips: int
    valid_trips: int
    days: int
    departures: int  # from the station counts
    returns: int

    def days_of_month(self) -> tuple[str, ...]:
        """Every calendar day of the month, "2019-06-01" to "2019-06-30" """
        year, number = (int(part) for part in self.month.split("-"))
        length = calendar.monthrange(year, number)[1]
        return tuple(f"{self.month}-{day:02d}" for day in range(1, length + 1))

    def slices(self) -> list[TableSlice]:
        """The items in the order they are written: raw trips first, the month's figures last"""
        return [
            TableSlice(TRIPS, self.raw_trips, self.days_of_month()),
            TableSlice(STATION_METRICS, self.stations, (self.month,)),
            TableSlice(DAILY_METRICS, self.daily, (self.month,)),
            TableSlice(MONTHLY_METRICS, self.monthly, ()),
        ]


def build_month(month: str, trips_csv: str, counts_csv: str) -> MonthItems:
    """The items for month, from its trips file and the Spark job's station counts"""
    trips = parse_trips(trips_csv)
    frame = with_validity(trips.frame)
    monthly = period_metrics(frame, MONTH)
    if list(monthly.index) != [month]:
        found = ", ".join(monthly.index) or "no trips at all"
        raise ValueError(f"the trips file for {month} has {found}")
    days = days_per_month(frame)
    stations = station_items(counts_csv, month)
    return MonthItems(
        month=month,
        raw_trips=list(raw_trip_items(trips)),
        daily=daily_items(period_metrics(frame, DAY)),
        monthly=monthly_items(monthly, days),
        stations=stations,
        trips=len(frame),
        valid_trips=int(frame["valid"].sum()),
        days=int(days[month]),
        departures=sum(item["departures"] for item in stations),
        returns=sum(item["returns"] for item in stations),
    )
