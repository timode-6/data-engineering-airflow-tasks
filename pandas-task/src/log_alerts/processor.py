"""The processor: picks up new journals, runs the rules and sends the alerts.

    python -m log_alerts process

It watches the incoming folder and handles journals in name order, which is
the order they were written in. A journal still being written has a name that
does not end in .csv yet, so it is never read half-way. Each journal is read
in chunks; every chunk goes through all the rules, and the alerts it raises
are sent right away. The journal then moves to the processed folder, or to
the failed one if it could not be read at all.
"""

from __future__ import annotations

import logging
import shutil
import signal
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from log_alerts.config import Settings
from log_alerts.journal import read_journal
from log_alerts.notifiers import (
    EmailNotifier,
    JsonLinesNotifier,
    LogNotifier,
    Notifier,
    notify_all,
)
from log_alerts.rules import Rule, load_rules

logger = logging.getLogger(__name__)

JOURNAL_PATTERN = "*.csv"


@dataclass
class Totals:
    """What the processor did over some stretch of work."""

    files: int = 0
    rows: int = 0
    rejected: int = 0
    alerts: int = 0
    seconds: float = 0.0  # time spent working, waits excluded

    def add(self, other: Totals) -> None:
        self.files += other.files
        self.rows += other.rows
        self.rejected += other.rejected
        self.alerts += other.alerts
        self.seconds += other.seconds

    def describe(self) -> str:
        speed = f", {self.rows / self.seconds:,.0f} rows/s" if self.seconds else ""
        return (
            f"{self.files:,} files, {self.rows:,} rows, {self.rejected:,} rejected, "
            f"{self.alerts:,} alerts, {self.seconds:.1f} s of work{speed}"
        )


class Progress:
    """Logs what was done every `every` seconds, also during a long backlog."""

    def __init__(self, every: float) -> None:
        self.every = every
        self.total = Totals()
        self._recent = Totals()
        self._since = time.monotonic()

    def add(self, done: Totals) -> None:
        self.total.add(done)
        self._recent.add(done)
        self.report_if_due()

    def report_if_due(self) -> None:
        elapsed = time.monotonic() - self._since
        if self._recent.files and elapsed >= self.every:
            logger.info("last %.0f s: %s", elapsed, self._recent.describe())
            self._recent, self._since = Totals(), time.monotonic()


class Processor:
    def __init__(
        self, rules: Sequence[Rule], notifiers: Sequence[Notifier], settings: Settings
    ) -> None:
        self.rules = rules
        self.notifiers = notifiers
        self.settings = settings

    def run(self, stop: threading.Event) -> Totals:
        """Process journals as they arrive, until `stop` is set."""
        logger.info("watching %s for journals", self.settings.incoming_dir)
        progress = Progress(self.settings.report_seconds)
        while not stop.is_set():
            waiting = self.waiting_journals()
            for path in waiting:
                if stop.is_set():
                    break
                progress.add(self.process_file(path))
            if not waiting:
                progress.report_if_due()
                stop.wait(self.settings.poll_seconds)
        logger.info("stopped after %s", progress.total.describe())
        return progress.total

    def waiting_journals(self) -> list[Path]:
        """The journals in the incoming folder, in name order."""
        return sorted(self.settings.incoming_dir.glob(JOURNAL_PATTERN))

    def process_waiting(self) -> Totals:
        """Process every journal waiting in the incoming folder, then return."""
        totals = Totals()
        for path in self.waiting_journals():
            totals.add(self.process_file(path))
        return totals

    def process_file(self, path: Path) -> Totals:
        """Run one journal through the rules and move it out of the incoming folder."""
        started = time.perf_counter()
        totals = Totals(files=1)
        chunks = read_journal(path, self.settings.chunk_rows)
        while True:
            # Only reading is guarded: a file that is not readable CSV is set
            # aside, while an error in a rule is a bug and should stop the run
            try:
                chunk = next(chunks, None)
            except (OSError, ValueError) as error:
                logger.error("%s could not be read, moved to failed: %s", path.name, error)
                if path.exists():
                    _move(path, self.settings.failed_dir)
                return totals
            if chunk is None:
                break
            totals.rows += len(chunk.events)
            totals.rejected += chunk.rejected
            alerts = [alert for rule in self.rules for alert in rule.evaluate(chunk.events)]
            alerts.sort(key=lambda alert: alert.last_event)
            totals.alerts += len(alerts)
            notify_all(self.notifiers, alerts)
        _move(path, self.settings.processed_dir)
        totals.seconds = time.perf_counter() - started
        if totals.rejected:
            logger.warning("%s: rejected %d malformed rows", path.name, totals.rejected)
        logger.debug("%s: %s", path.name, totals.describe())
        return totals


def _move(path: Path, folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    shutil.move(path, folder / path.name)


def build_notifiers(settings: Settings) -> list[Notifier]:
    notifiers: list[Notifier] = [LogNotifier(), JsonLinesNotifier(settings.alerts_file)]
    if settings.smtp_host:
        notifiers.append(
            EmailNotifier(
                settings.smtp_host, settings.smtp_port, settings.email_from, settings.email_to
            )
        )
    return notifiers


def main() -> None:
    try:
        settings = Settings.from_env()
        rules = load_rules(settings.rules_file)
    except (OSError, ValueError) as error:
        raise SystemExit(f"configuration error: {error}") from None

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    notifiers = build_notifiers(settings)
    settings.incoming_dir.mkdir(parents=True, exist_ok=True)
    logger.info("rules: %s", ", ".join(rule.name for rule in rules))
    logger.info("alerts go to: %s", ", ".join(notifier.name for notifier in notifiers))

    # `docker compose stop` sends SIGTERM: finish the current journal, then exit
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    Processor(rules, notifiers, settings).run(stop)
