"""Print what the Lambda would load for one month, from local files, without AWS

    python -m bikes.loader.preview data/processed/trips_2019-06.csv
    python -m bikes.loader.preview data/processed/trips_2019-06.csv \\
        --counts data/processed/station_counts_2019-06.csv
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from bikes.loader.items import raw_trip_items, station_items
from bikes.loader.metrics import DAY, MONTH, days_per_month, period_metrics, with_validity
from bikes.loader.trips import parse_trips


def figures_line(label: str, row: pd.Series) -> str:
    """One period's figures in aligned columns; durations in minutes, easier to read."""
    trips, valid, hours = int(row["trips"]), int(row["valid_trips"]), int(row["temperature_hours"])
    share = valid / trips if trips else 0
    return (
        f"{label:<10} {trips:>9,} {valid:>9,} {share:>6.1%}"
        f" {row['avg_distance_m']:>8,.0f} {row['avg_duration_s'] / 60:>6.1f}"
        f" {row['avg_speed_kmh']:>6.1f} {row['avg_temperature_c']:>6.1f} {hours:>5}"
    )


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m bikes.loader.preview",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("trips", type=Path, help="a monthly trips file")
    parser.add_argument("--counts", type=Path, help="the Spark job's file for the same month")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    started = time.monotonic()
    trips = parse_trips(args.trips.read_text(encoding="utf-8"))
    frame = with_validity(trips.frame)
    daily, monthly = period_metrics(frame, DAY), period_metrics(frame, MONTH)
    days = days_per_month(frame)

    print(f"{args.trips.name}: {len(frame):,} trips, read in {time.monotonic() - started:.1f} s\n")
    print(
        f"{'':<10} {'trips':>9} {'valid':>9} {'share':>6} {'dist m':>8} {'min':>6}"
        f" {'km/h':>6} {'°C':>6} {'hours':>5}"
    )
    for month, row in monthly.iterrows():
        print(figures_line(month, row) + f"   {days[month]} days")
    print()
    for day, row in daily.iterrows():
        print(figures_line(day, row))

    started = time.monotonic()
    items = list(raw_trip_items(trips))
    packed = sum(len(item["trips_csv_gz"]) for item in items)
    largest = max(len(item["trips_csv_gz"]) for item in items)
    extra_parts = sum(item["part"] > 0 for item in items)
    print(
        f"\nRaw trips: {len(items):,} items ({extra_parts:,} of them extra parts of busy days),"
        f" {packed / 2**20:,.1f} MiB compressed, largest {largest / 1024:,.0f} KB;"
        f" built in {time.monotonic() - started:.1f} s"
    )

    if args.counts:
        month = days.index[0]
        stations = station_items(args.counts.read_text(encoding="utf-8"), month)
        departures = sum(item["departures"] for item in stations)
        returns = sum(item["returns"] for item in stations)
        agree = "agree" if departures == returns == len(frame) else "DO NOT AGREE"
        print(
            f"Station counts: {len(stations)} stations, {departures:,} departures and "
            f"{returns:,} returns; Spark and pandas {agree} on the number of trips"
        )


if __name__ == "__main__":
    main()
