"""Log rows built in code, shared by the tests."""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable

import pandas as pd

from log_alerts.alerts import Alert, to_utc
from log_alerts.journal import COLUMNS, TIME_COLUMN

# A row from the real dataset, used as the template for every test row
TEMPLATE = dict(
    zip(
        COLUMNS,
        [
            "0", "could not receive ip address from ipfy", "Error", "/BuildNotice/0",
            "MobileSDK", "Cherry_Mobile A890", "OpenGLES2", "1.0", "1.11.0", "False",
            "LowRevenueCountryBlock", "LowRevenueCountryBlock", "9/12/2021 10:31:18 AM",
            "6028e36ad1e80652e41054ca", "6028e407a1ba2a0d3d760566",
            "com.mytalkingcatemma.ballerinagames", "1.0", "en",
            "Android OS 10 / API-29 (Q00711/1594896502)",
            "6f4d7e21-1060-44ce-b032-5a5052a078cd", "False", "False", "MA",
            "1631442680.584186",
        ],
        strict=True,
    )
)  # fmt: skip

START = 1631442680.0  # 2021-09-12 10:31:20 UTC


def make_row(**fields: object) -> list[str]:
    """A log row as a list of 24 strings, with any column overridden."""
    unknown = set(fields) - set(COLUMNS)
    if unknown:
        raise ValueError(f"not a column: {sorted(unknown)}")
    row = {**TEMPLATE, **{name: str(value) for name, value in fields.items()}}
    return [row[name] for name in COLUMNS]


def make_events(rows: Iterable[list[str]]) -> pd.DataFrame:
    """Rows as the journal reader returns them: text columns, `date` as a float.

    They go through CSV and pandas the same way, so an empty field becomes a
    missing value here too.
    """
    buffer = io.StringIO()
    csv.writer(buffer).writerows(rows)
    buffer.seek(0)
    events = pd.read_csv(buffer, header=None, names=list(COLUMNS), dtype=str)
    return events.assign(**{TIME_COLUMN: events[TIME_COLUMN].astype(float)})


def errors_at(*seconds: float, **fields: object) -> pd.DataFrame:
    """Error rows logged the given number of seconds after START."""
    return make_events(make_row(date=START + second, **fields) for second in seconds)


def spread(count: int, span: float) -> list[float]:
    """`count` times evenly spread from 0 to `span` seconds."""
    return [span * i / (count - 1) for i in range(count)]


def make_alert(**fields: object) -> Alert:
    """An alert like the per-minute rule raises, with any field overridden."""
    values: dict[str, object] = {
        "rule": "fatal_errors_per_minute",
        "description": "More than 10 fatal errors in less than one minute",
        "count": 23,
        "first_event": to_utc(START),
        "last_event": to_utc(START + 41.5),
        **fields,
    }
    return Alert(**values)
