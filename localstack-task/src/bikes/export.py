"""Export the metrics tables to CSV files, for the Tableau Public dashboards"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from bikes import aws
from bikes.config import AwsSettings, ConfigError
from bikes.infra import table_names
from bikes.loader.stores import LOADED, Ledger, all_items
from bikes.resources import DAILY_METRICS, LOADS, MONTHLY_METRICS, STATION_METRICS, TableSpec

OUT = Path("data/export")
ENCODING = "utf-8-sig"  # a BOM, so Excel and Tableau on Windows read station names right

COUNTS = {"trips": "Trips", "valid_trips": "Valid trips"}
# Attribute: (column, factor to the column's unit, decimals kept)
AVERAGES = {
    "avg_distance_m": ("Avg distance (km)", 1 / 1000, 3),
    "avg_duration_s": ("Avg duration (min)", 1 / 60, 2),
    "avg_speed_kmh": ("Avg speed (km/h)", 1, 2),
    "avg_temperature_c": ("Avg temperature (degC)", 1, 2),
}
HOURS = {"temperature_hours": "Temperature hours"}

Report = Callable[[str], None]


def first_of_month(months: pd.Series) -> pd.Series:
    """2019-06 as 2019-06-01, a date Tableau recognises"""
    return months + "-01"


def figures(items: pd.DataFrame) -> pd.DataFrame:
    """The counts and the four averages of metrics items, in chart units and names"""
    columns = {label: items[name].map(int) for name, label in COUNTS.items()}
    for name, (label, factor, decimals) in AVERAGES.items():
        columns[label] = (items[name].astype(float) * factor).round(decimals)
    columns |= {label: items[name].map(int) for name, label in HOURS.items()}
    return pd.DataFrame(columns, index=items.index)


def daily_rows(records: list[dict[str, Any]]) -> pd.DataFrame:
    """bike_daily_metrics items as rows: Date, then the figures"""
    items = pd.DataFrame(records, columns=["trip_date", *COUNTS, *AVERAGES, *HOURS])
    rows = pd.concat([pd.DataFrame({"Date": items["trip_date"]}), figures(items)], axis=1)
    return rows.sort_values("Date", ignore_index=True)


def monthly_rows(records: list[dict[str, Any]]) -> pd.DataFrame:
    """bike_monthly_metrics items as rows: Month, the days it had trips, the figures"""
    items = pd.DataFrame(records, columns=["trip_month", "days", *COUNTS, *AVERAGES, *HOURS])
    first = pd.DataFrame(
        {"Month": first_of_month(items["trip_month"]), "Days with trips": items["days"].map(int)}
    )
    rows = pd.concat([first, figures(items)], axis=1)
    return rows.sort_values("Month", ignore_index=True)


def station_rows(records: list[dict[str, Any]]) -> pd.DataFrame:
    """bike_station_metrics items as rows: Month, Station, Departures, Returns"""
    items = pd.DataFrame(records, columns=["trip_month", "station_name", "departures", "returns"])
    rows = pd.DataFrame(
        {
            "Month": first_of_month(items["trip_month"]),
            "Station": items["station_name"],
            "Departures": items["departures"].map(int),
            "Returns": items["returns"].map(int),
        }
    )
    return rows.sort_values(["Month", "Station"], ignore_index=True)


EXPORTS: dict[TableSpec, Callable[[list[dict[str, Any]]], pd.DataFrame]] = {
    DAILY_METRICS: daily_rows,
    MONTHLY_METRICS: monthly_rows,
    STATION_METRICS: station_rows,
}


def export(dynamodb: Any, out: Path, report: Report = print) -> list[Path]:
    """Write a CSV file per metrics table into out; returns the files"""
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for spec, to_rows in EXPORTS.items():
        rows = to_rows(list(all_items(dynamodb.Table(spec.name).scan)))
        path = out / f"{spec.name}.csv"
        rows.to_csv(path, index=False, encoding=ENCODING)
        span = f", {rows.iloc[0, 0]} to {rows.iloc[-1, 0]}" if len(rows) else ""
        report(f"{path}: {len(rows):,} rows{span}")
        written.append(path)
    return written


def unfinished_months(entries: Sequence[dict[str, Any]]) -> list[str]:
    """The months of bike_loads entries that are not loaded: loading, or failed"""
    return [entry[LOADS.partition_key] for entry in entries if entry["load_state"] != LOADED]


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m bikes.export",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--out", type=Path, default=OUT, help=f"where the files go ({OUT})")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        AwsSettings.from_env()
    except ConfigError as error:
        raise SystemExit(str(error)) from error
    missing = {spec.name for spec in [*EXPORTS, LOADS]} - table_names(aws.client("dynamodb"))
    if missing:
        raise SystemExit(
            f"Missing: {', '.join(sorted(missing))}. Run `python -m bikes.infra up`, "
            "then deploy the Lambda and load the months"
        )
    dynamodb = aws.resource("dynamodb")
    entries = Ledger(dynamodb.Table(LOADS.name)).entries()
    if not entries:
        raise SystemExit(
            f"Nothing is loaded yet, so the files would be empty; {args.out} is left as it is. "
            "Load the months first (docs/loader.md)"
        )
    unfinished = unfinished_months(entries)
    if unfinished:
        print(
            f"Not loaded: {', '.join(unfinished)}. Their rows may be missing or out of date; "
            "`python -m bikes.infra loads` says why"
        )
    export(dynamodb, args.out)


if __name__ == "__main__":
    main()
