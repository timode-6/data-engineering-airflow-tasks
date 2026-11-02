"""Reading log journals.

A journal is a CSV file without a header, one log record per row, in the
24 columns below. Fields may be quoted, and a quoted error message can contain
commas and line breaks.
"""

from __future__ import annotations

import time
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

COLUMNS = (
    "error_code", "error_message", "severity", "log_location", "mode", "model",
    "graphics", "session_id", "sdkv", "test_mode", "flow_id", "flow_type",
    "sdk_date", "publisher_id", "game_id", "bundle_id", "appv", "language",
    "os", "adv_id", "gdpr", "ccpa", "country_code", "date",
)  # fmt: skip

# `date` is the time the record was logged, in seconds since the Unix epoch.
TIME_COLUMN = "date"

# A `date` before 2001-09-09 cannot be a log time. This also drops the
# dataset's first line, which holds the column numbers 0-23.
EARLIEST_TIME = 1_000_000_000

# A `date` more than a day ahead of the clock is treated as broken too.
MAX_SECONDS_AHEAD = 86_400


@dataclass(frozen=True)
class Chunk:
    """Valid rows from one part of a journal.

    `events` holds all 24 columns as strings, except `date`, which is a float.
    `rejected` counts the rows dropped from this part: lines with the wrong
    number of fields, and rows without a usable `date` or `severity`.
    """

    events: pd.DataFrame
    rejected: int


def read_journal(path: Path, chunk_rows: int = 100_000) -> Iterator[Chunk]:
    """Yield the rows of a journal in chunks of at most `chunk_rows` rows.

    All 24 columns are read, rather than only those the rules need, for two
    reasons: pandas skips a line with an extra field only when it reads every
    column (with `usecols` it shifts the values silently), and a new rule can
    then use any column.
    """
    reader = pd.read_csv(
        path,
        header=None,
        names=list(COLUMNS),
        dtype=str,
        chunksize=chunk_rows,
        on_bad_lines="warn",
        encoding_errors="replace",
    )
    with reader:
        while True:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", pd.errors.ParserWarning)
                chunk = next(reader, None)
            malformed = _count_skipped_lines(caught)
            if chunk is None:
                if malformed:
                    yield Chunk(pd.DataFrame(columns=list(COLUMNS)), malformed)
                return
            events, invalid = _keep_valid(chunk)
            yield Chunk(events, malformed + invalid)


def _count_skipped_lines(caught: list[warnings.WarningMessage]) -> int:
    # pandas reports each skipped line as "Skipping line N: expected 24 fields, saw 25"
    return sum(
        str(warning.message).count("Skipping line")
        for warning in caught
        if issubclass(warning.category, pd.errors.ParserWarning)
    )


def _keep_valid(chunk: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    times = pd.to_numeric(chunk[TIME_COLUMN], errors="coerce")
    latest = time.time() + MAX_SECONDS_AHEAD
    valid = times.between(EARLIEST_TIME, latest) & chunk["severity"].notna()
    if valid.all():  # the usual case, and copying 24 columns for nothing is slow
        return chunk.assign(**{TIME_COLUMN: times}), 0
    events = chunk.loc[valid].assign(**{TIME_COLUMN: times[valid]})
    return events, int((~valid).sum())
