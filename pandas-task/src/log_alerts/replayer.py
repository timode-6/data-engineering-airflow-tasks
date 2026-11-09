"""The replayer: plays the mobile app, sending the dataset as journals.

    python -m log_alerts replay

The dataset is a dump in which four days of logs are shuffled together, so
it has to be put in time order first. That happens in two passes, so memory
holds one hour of logs rather than the whole file:

1. the rows are split into one file per hour of log time;
2. each hour is sorted by `date` and cut into journals of JOURNAL_SECONDS.

A journal goes to the incoming folder once its stretch of log time has
passed, at SPEED times real time. It is written under a .part name and
renamed when complete, so the processor never reads half of one.
"""

from __future__ import annotations

import logging
import shutil
import signal
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from log_alerts.config import ReplaySettings
from log_alerts.journal import TIME_COLUMN, read_journal

logger = logging.getLogger(__name__)

HOUR = 3600


@dataclass
class ReplayTotals:
    journals: int = 0
    rows: int = 0
    rejected: int = 0


def replay(settings: ReplaySettings, stop: threading.Event) -> ReplayTotals:
    """Write the source file to the incoming folder as journals, in time order."""
    totals = ReplayTotals()
    started = time.monotonic()
    logger.info("sorting %s by log time", settings.source_file)
    try:
        hours = split_by_hour(settings, totals)
        logger.info(
            "sorted %s rows (%s rejected) into %d hours of logs in %.1f s",
            f"{totals.rows:,}",
            f"{totals.rejected:,}",
            len(hours),
            time.monotonic() - started,
        )
        settings.incoming_dir.mkdir(parents=True, exist_ok=True)
        clock_start = time.monotonic()
        first_start: float | None = None
        for start, journal in journals(hours, settings):
            if first_start is None:
                first_start = start
            if settings.speed:
                due = (start + settings.journal_seconds - first_start) / settings.speed
                delay = due - (time.monotonic() - clock_start)
                if delay > 0 and stop.wait(delay):
                    break
            if stop.is_set():
                break
            write_journal(journal, start, settings.incoming_dir)
            totals.journals += 1
    finally:
        shutil.rmtree(settings.staging_dir, ignore_errors=True)
    logger.info(
        "replayed %s journals with %s rows in %.0f s",
        f"{totals.journals:,}",
        f"{totals.rows:,}",
        time.monotonic() - started,
    )
    return totals


def split_by_hour(settings: ReplaySettings, totals: ReplayTotals) -> list[Path]:
    """First pass: one CSV per hour of log time. Returns them oldest first."""
    shutil.rmtree(settings.staging_dir, ignore_errors=True)
    settings.staging_dir.mkdir(parents=True)
    for chunk in read_journal(settings.source_file, settings.chunk_rows):
        totals.rows += len(chunk.events)
        totals.rejected += chunk.rejected
        hour = (chunk.events[TIME_COLUMN] // HOUR).astype("int64")
        for number, rows in chunk.events.groupby(hour):
            _write_csv(rows, settings.staging_dir / f"{number}.csv", append=True)
    return sorted(settings.staging_dir.glob("*.csv"), key=lambda path: int(path.stem))


def journals(hours: list[Path], settings: ReplaySettings) -> Iterator[tuple[float, pd.DataFrame]]:
    """Second pass: each hour sorted by time and cut into journals, oldest first."""
    for path in hours:
        events = pd.concat(chunk.events for chunk in read_journal(path, settings.chunk_rows))
        events = events.sort_values(TIME_COLUMN, kind="stable")
        slot = (events[TIME_COLUMN] // settings.journal_seconds).astype("int64")
        for number, rows in events.groupby(slot, sort=True):
            yield number * settings.journal_seconds, rows


def write_journal(rows: pd.DataFrame, start: float, folder: Path) -> Path:
    """Write a journal named after the start of its stretch of log time."""
    name = datetime.fromtimestamp(start, tz=UTC).strftime("journal_%Y%m%dT%H%M%S.csv")
    partial = folder / f"{name}.part"
    _write_csv(rows, partial)
    return partial.replace(folder / name)


def _write_csv(rows: pd.DataFrame, path: Path, append: bool = False) -> None:
    # `date` back to text with microseconds, as the app writes it
    text = rows.assign(**{TIME_COLUMN: rows[TIME_COLUMN].map("{:.6f}".format)})
    text.to_csv(path, mode="a" if append else "w", header=False, index=False)


def main() -> None:
    try:
        settings = ReplaySettings.from_env()
    except ValueError as error:
        raise SystemExit(f"configuration error: {error}") from None
    if not settings.source_file.is_file():
        raise SystemExit(f"no dataset at {settings.source_file}: see the README")

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    replay(settings, stop)
