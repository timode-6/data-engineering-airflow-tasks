from __future__ import annotations

import csv
import threading
import time
from pathlib import Path

import pytest

from helpers import START, make_row
from log_alerts import processor as processor_module
from log_alerts.config import Settings
from log_alerts.notifiers import EmailNotifier, JsonLinesNotifier, LogNotifier, Notifier
from log_alerts.processor import Processor, build_notifiers
from log_alerts.rules import ThresholdRule


class RecordingNotifier(Notifier):
    name = "recording"

    def __init__(self) -> None:
        self.alerts = []

    def send(self, alerts) -> None:
        self.alerts.extend(alerts)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        incoming_dir=tmp_path / "incoming",
        processed_dir=tmp_path / "processed",
        failed_dir=tmp_path / "failed",
        alerts_file=tmp_path / "alerts" / "alerts.jsonl",
        poll_seconds=0.05,
        chunk_rows=4,
    )


@pytest.fixture
def recorder() -> RecordingNotifier:
    return RecordingNotifier()


@pytest.fixture
def processor(settings: Settings, recorder: RecordingNotifier) -> Processor:
    settings.incoming_dir.mkdir()
    rule = ThresholdRule(
        "per_minute", "More than 10 errors in less than one minute",
        where={"severity": "Error"}, window=60, threshold=10,
    )  # fmt: skip
    return Processor([rule], [recorder], settings)


def write_journal(folder: Path, name: str, *seconds: float, **fields: object) -> Path:
    path = folder / name
    with path.open("w", encoding="utf-8", newline="") as file:
        csv.writer(file).writerows(make_row(date=START + s, **fields) for s in seconds)
    return path


def names(folder: Path) -> list[str]:
    return sorted(path.name for path in folder.iterdir()) if folder.exists() else []


def test_journals_go_in_name_order_and_end_up_in_processed(
    processor, settings, recorder, monkeypatch
):
    write_journal(settings.incoming_dir, "journal_002.csv", 18, 21, 24, 27, 30)
    write_journal(settings.incoming_dir, "journal_001.csv", 0, 3, 6, 9, 12, 15)
    read_order = []
    real_read_journal = processor_module.read_journal

    def recording_read_journal(path, chunk_rows):
        read_order.append(path.name)
        return real_read_journal(path, chunk_rows)

    monkeypatch.setattr(processor_module, "read_journal", recording_read_journal)

    totals = processor.process_waiting()

    assert read_order == ["journal_001.csv", "journal_002.csv"]
    assert (totals.files, totals.rows, totals.alerts) == (2, 11, 1)
    assert [alert.count for alert in recorder.alerts] == [11]  # one burst across two files
    assert names(settings.incoming_dir) == []
    assert names(settings.processed_dir) == ["journal_001.csv", "journal_002.csv"]


def test_a_journal_still_being_written_is_left_alone(processor, settings):
    write_journal(settings.incoming_dir, "journal_003.csv.part", 0, 1, 2)

    assert processor.process_waiting().files == 0
    assert names(settings.incoming_dir) == ["journal_003.csv.part"]


def test_rejected_rows_are_counted(processor, settings):
    path = write_journal(settings.incoming_dir, "journal_001.csv", 0, 1)
    with path.open("a", encoding="utf-8") as file:
        file.write("0,1,2,3\n")

    totals = processor.process_waiting()

    assert (totals.rows, totals.rejected) == (2, 1)


def test_an_unreadable_journal_moves_to_failed_and_the_rest_carry_on(processor, settings):
    (settings.incoming_dir / "journal_001.csv").mkdir()  # a folder, not a file
    write_journal(settings.incoming_dir, "journal_002.csv", 0, 1, 2)

    totals = processor.process_waiting()

    assert totals.rows == 3
    assert names(settings.failed_dir) == ["journal_001.csv"]
    assert names(settings.processed_dir) == ["journal_002.csv"]


def test_run_picks_up_new_journals_until_stopped(processor, settings, recorder):
    stop = threading.Event()
    thread = threading.Thread(target=processor.run, args=(stop,))
    thread.start()
    try:
        write_journal(settings.incoming_dir, "journal_001.csv", *range(11))
        deadline = time.monotonic() + 5
        while not names(settings.processed_dir) and time.monotonic() < deadline:
            time.sleep(0.02)
    finally:
        stop.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert names(settings.processed_dir) == ["journal_001.csv"]
    assert [alert.count for alert in recorder.alerts] == [11]


def test_email_is_used_only_when_smtp_host_is_set(settings):
    without = build_notifiers(settings)
    with_email = build_notifiers(Settings(smtp_host="mailpit"))

    assert [type(n) for n in without] == [LogNotifier, JsonLinesNotifier]
    assert [type(n) for n in with_email] == [LogNotifier, JsonLinesNotifier, EmailNotifier]
