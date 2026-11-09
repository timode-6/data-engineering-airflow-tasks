"""Builders for raw rows in the dataset's own format, written with the csv module
rather than pandas, so the code under test never reads its own output"""

from __future__ import annotations

import csv
import io
from collections import Counter
from pathlib import Path

from bikes.dataset import COLUMNS, DEPARTURE_NAME, RETURN_NAME

# The first line of the real file
FIRST_LINE = (
    "2020-03-23 06:09:44,2020-03-23 06:16:26,86,Kuusitie,111.0,Esterinportti,1747.0,401.0,"
    "0.2613965087281795,60.1952452,24.9018997,60.1975724,24.9267808,0.9"
)
FIRST_ROW = dict(zip(COLUMNS, next(csv.reader([FIRST_LINE])), strict=True))

# Short names for the columns whose own names are not valid keyword arguments
ALIASES = {
    "distance": "distance (m)",
    "duration": "duration (sec.)",
    "speed": "avg_speed (km/h)",
    "temperature": "Air temperature (degC)",
}


def trip(departure: str, **fields: str) -> dict[str, str]:
    """FIRST_ROW with another departure and any other fields replaced."""
    row = {**FIRST_ROW, "departure": departure}
    for name, value in fields.items():
        row[ALIASES.get(name, name)] = value
    return row


def write_source(
    directory: Path, rows: list[dict[str, str]], name: str = "database.csv", encoding="utf-8"
) -> Path:
    """Write rows under the dataset's header and return the path"""
    path = directory / name
    with path.open("w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def source_text(rows: list[dict[str, str]]) -> str:
    """The text write_source would write: a monthly trips file as it sits in S3"""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def counts_text(rows: list[dict[str, str]]) -> str:
    """What the Spark job writes for these trips: departures and returns per station"""
    departures = Counter(row[DEPARTURE_NAME] for row in rows)
    returns = Counter(row[RETURN_NAME] for row in rows)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["station_name", "departures", "returns"])
    for station in sorted(departures.keys() | returns.keys()):
        writer.writerow([station, departures[station], returns[station]])
    return buffer.getvalue()


def read_rows(path: Path) -> list[dict[str, str]]:
    """Every row of a CSV file as text"""
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))
