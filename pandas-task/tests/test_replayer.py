from __future__ import annotations

import csv
import dataclasses
import threading
from pathlib import Path

import pandas as pd
import pytest

from helpers import START, make_row
from log_alerts.config import ReplaySettings, Settings
from log_alerts.journal import COLUMNS, read_journal
from log_alerts.processor import Processor
from log_alerts.replayer import replay
from log_alerts.rules import ThresholdRule

MESSAGE = 'We are running "Programmatic Flow", you need to active gCom.\n HTTP/1.1 400 Bad Request'


class RecordingStop(threading.Event):
    """A stop event that records the waits instead of sleeping through them."""

    def __init__(self) -> None:
        super().__init__()
        self.waits: list[float] = []

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout)
        return self.is_set()


def write_source(path: Path, rows: list[list[str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        csv.writer(file).writerows(rows)
    return path


@pytest.fixture
def settings(tmp_path: Path) -> ReplaySettings:
    return ReplaySettings(
        source_file=tmp_path / "source" / "data.csv",
        incoming_dir=tmp_path / "incoming",
        staging_dir=tmp_path / "replay",
        journal_seconds=60,
        speed=0,
        chunk_rows=3,
    )


def read_back(folder: Path) -> list[pd.DataFrame]:
    return [
        pd.concat(chunk.events for chunk in read_journal(path))
        for path in sorted(folder.glob("*.csv"))
    ]


def test_shuffled_days_come_out_as_sorted_one_minute_journals(settings):
    # As in the dataset: the column numbers first, then days mixed together
    seconds = [86400 + 5, 30, 86400 + 70, 65, 3, 86400 + 1, 130]
    rows = [[str(i) for i in range(len(COLUMNS))]]
    rows += [make_row(date=START + s, error_message=f"row {s}") for s in seconds]
    write_source(settings.source_file, rows)

    totals = replay(settings, threading.Event())

    names = sorted(path.name for path in settings.incoming_dir.iterdir())
    assert names == [
        "journal_20210912T103100.csv",  # START is 10:31:20, so 3 s and 30 s fall in 10:31
        "journal_20210912T103200.csv",
        "journal_20210912T103300.csv",
        "journal_20210913T103100.csv",
        "journal_20210913T103200.csv",
    ]
    dates = [list(journal["date"] - START) for journal in read_back(settings.incoming_dir)]
    assert dates == [[3, 30], [65], [130], [86400 + 1, 86400 + 5], [86400 + 70]]
    assert (totals.journals, totals.rows, totals.rejected) == (5, 7, 1)
    assert not settings.staging_dir.exists()


def test_every_field_survives_the_replay(settings):
    original = make_row(date=START + 0.584186, error_message=MESSAGE, bundle_id="com.example.game")
    write_source(settings.source_file, [original])

    replay(settings, threading.Event())

    (journal,) = read_back(settings.incoming_dir)
    replayed = journal.iloc[0].tolist()
    assert replayed[:-1] == original[:-1]
    assert replayed[-1] == pytest.approx(START + 0.584186)
    assert list(settings.incoming_dir.glob("*.part")) == []


def test_journals_are_spaced_by_speed(settings):
    write_source(settings.source_file, [make_row(date=START + s) for s in (0, 60, 120)])
    stop = RecordingStop()

    replay(dataclasses.replace(settings, speed=60), stop)

    # at 60 times real time each one-minute journal is due a second after the last
    assert stop.waits == pytest.approx([1, 2, 3], abs=0.5)


def test_stop_ends_the_replay(settings):
    write_source(settings.source_file, [make_row(date=START + s) for s in (0, 60, 120)])
    stop = threading.Event()
    stop.set()

    totals = replay(settings, stop)

    assert totals.journals == 0
    assert not settings.staging_dir.exists()


def test_a_burst_across_two_journals_raises_one_alert_end_to_end(settings, tmp_path):
    # 11 errors in 15 s: six before 10:32:00, five after
    seconds = [30, 32, 34, 36, 38, 39, 41, 42, 43, 44, 45]
    write_source(settings.source_file, [make_row(date=START + s) for s in reversed(seconds)])
    replay(settings, threading.Event())
    rule = ThresholdRule(
        "per_minute", "More than 10 errors in less than one minute",
        where={"severity": "Error"}, window=60, threshold=10,
    )  # fmt: skip
    processor = Processor(
        [rule],
        [],
        Settings(incoming_dir=settings.incoming_dir, processed_dir=tmp_path / "processed"),
    )

    totals = processor.process_waiting()

    assert (totals.files, totals.rows, totals.alerts) == (2, 11, 1)
