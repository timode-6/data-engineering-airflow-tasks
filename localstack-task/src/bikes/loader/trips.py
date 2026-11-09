"""A month's trips file, read once, its lines as written for the raw table,
and the few typed columns the metrics need"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass

import pandas as pd

from bikes.dataset import COLUMNS, DEPARTURE, DEPARTURE_NAME, DISTANCE, DURATION, TEMPERATURE

TYPED = {
    DEPARTURE: "departure",
    DEPARTURE_NAME: "station",
    DISTANCE: "distance_m",
    DURATION: "duration_s",
    TEMPERATURE: "temperature_c",
}


@dataclass(frozen=True)
class MonthTrips:
    """One monthly file: its header, its lines as written, and typed columns"""

    header: str
    lines: list[str]
    frame: pd.DataFrame


def parse_trips(text: str) -> MonthTrips:
    """Read a monthly trips file from its text"""
    text = text.replace("\r\n", "\n")
    header, *lines = text.rstrip("\n").split("\n")
    found = next(csv.reader([header]))
    if found != list(COLUMNS):
        raise ValueError(f"unexpected header {found}")

    raw = pd.read_csv(io.StringIO(text), usecols=list(TYPED), dtype=str, keep_default_na=False)
    if len(raw) != len(lines):
        raise ValueError(f"{len(lines):,} lines but pandas read {len(raw):,} rows")

    frame = pd.DataFrame(
        {
            "departure": pd.to_datetime(raw[DEPARTURE], format="ISO8601"),
            "station": raw[DEPARTURE_NAME],
            "distance_m": pd.to_numeric(raw[DISTANCE], errors="coerce"),
            "duration_s": pd.to_numeric(raw[DURATION], errors="coerce"),
            "temperature_c": pd.to_numeric(raw[TEMPERATURE], errors="coerce"),
        }
    )
    return MonthTrips(header=header, lines=lines, frame=frame)
