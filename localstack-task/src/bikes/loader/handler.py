"""The loader Lambda: loads a month into DynamoDB once both of its files are in S3

For each S3 event on the queue it
1. checks that the month's other file is in S3; if not, the message comes
   back in WAIT_SECONDS, which is how the function waits;
2. claims the month in bike_loads, so it is loaded once although both files'
   events arrive for it;
3. reads both files, writes the month's items, and records the load.

"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from bikes import aws
from bikes.loader.month import build_month
from bikes.loader.stores import Ledger, MonthFiles, Queue, S3Files, Writer, utc_now
from bikes.resources import LOADER_TIMEOUT, LOADS, FileKind, S3File
from bikes.s3_events import object_keys

LOG = logging.getLogger(__name__)

WAIT_SECONDS = 30
CLAIM_HOLD = timedelta(seconds=LOADER_TIMEOUT)

Clock = Callable[[], datetime]
BatchResponse = dict[str, list[dict[str, str]]]


class Outcome(StrEnum):
    """What becomes of one message"""

    DONE = "done"
    WAIT = "wait"


@dataclass(frozen=True)
class LoadSummary:
    """What one load did, and where its time went"""

    trips: int
    valid_trips: int
    days: int
    stations: int
    raw_items: int
    removed_items: int
    read_seconds: float
    build_seconds: float
    write_seconds: float  # DynamoDB

    @property
    def load_seconds(self) -> float:
        return self.read_seconds + self.build_seconds + self.write_seconds

    def counts(self) -> dict[str, int | Decimal]:
        """The same, with the total, as attributes for DynamoDB, which takes no floats"""
        values = {**asdict(self), "load_seconds": self.load_seconds}
        return {
            name: Decimal(f"{value:.1f}") if isinstance(value, float) else value
            for name, value in values.items()
        }

    def __str__(self) -> str:
        removed = f", removed {self.removed_items:,} left from before" if self.removed_items else ""
        return (
            f"loaded {self.trips:,} trips ({self.valid_trips:,} valid) on {self.days} days, "
            f"{self.stations} stations, {self.raw_items:,} raw items{removed} "
            f"in {self.load_seconds:.1f} s: S3 {self.read_seconds:.1f} s, "
            f"pandas {self.build_seconds:.1f} s, DynamoDB {self.write_seconds:.1f} s"
        )


class MonthLoader:
    """Turns the S3 events on the queue into loaded months"""

    def __init__(
        self, files: S3Files, ledger: Ledger, writer: Writer, queue: Queue, clock: Clock = utc_now
    ) -> None:
        self.files = files
        self.ledger = ledger
        self.writer = writer
        self.queue = queue
        self.clock = clock

    @classmethod
    def from_aws(cls) -> MonthLoader:
        """A loader on real clients"""
        connect = functools.partial(aws.resource, "dynamodb")
        return cls(
            files=S3Files(aws.client("s3")),
            ledger=Ledger(connect().Table(LOADS.name)),
            writer=Writer(connect),
            queue=Queue(aws.client("sqs")),
        )

    def handle_batch(self, event: Mapping[str, Any]) -> BatchResponse:
        """Handle every message the trigger sent"""
        failures = []
        for record in event.get("Records", []):
            try:
                outcome = self.handle_message(record)
            except Exception:
                LOG.exception("message %s failed; the queue delivers it again", record["messageId"])
                outcome = None
            if outcome is not Outcome.DONE:
                failures.append({"itemIdentifier": record["messageId"]})
        return {"batchItemFailures": failures}

    def handle_message(self, record: Mapping[str, Any]) -> Outcome:
        """One SQS message: an S3 event, which names one file"""
        keys = object_keys(record["body"])
        if keys is None:
            LOG.info("S3 test event: nothing to load")
            return Outcome.DONE
        outcomes = [self.handle_file(key) for key in keys]
        if Outcome.WAIT not in outcomes:
            return Outcome.DONE
        self.queue.retry_in(record["eventSourceARN"], record["receiptHandle"], WAIT_SECONDS)
        received = record.get("attributes", {}).get("ApproximateReceiveCount", "?")
        LOG.info("back in the queue for %d s (received %s times)", WAIT_SECONDS, received)
        return Outcome.WAIT

    def handle_file(self, key: str) -> Outcome:
        """Load the month of the file at key, if both of its files are in S3"""
        file = S3File.from_key(key)
        if file is None:
            LOG.warning("%s is not a month's trips or station counts; ignored", key)
            return Outcome.DONE
        etags = {kind: self.files.etag(S3File(kind, file.month).key) for kind in FileKind}
        if etags[file.kind] is None:
            LOG.warning("%s is no longer in S3; nothing to load", key)
            return Outcome.DONE
        if etags[file.kind.partner] is None:
            LOG.info("%s: waiting for %s", file.month, file.partner.key)
            return Outcome.WAIT
        return self.load_once(MonthFiles(file.month, etags[FileKind.RAW], etags[FileKind.METRICS]))

    def load_once(self, files: MonthFiles) -> Outcome:
        """Load the month from files, unless that is done already or under way"""
        claim = self.ledger.claim(files, self.clock(), CLAIM_HOLD)
        if claim is None:
            if self.ledger.is_loaded(files):
                LOG.info("%s: already loaded from these files; skipped", files.month)
                return Outcome.DONE
            LOG.info("%s: another call is loading it", files.month)
            return Outcome.WAIT
        try:
            summary = self.load(files)
        except Exception as error:
            self.ledger.fail(claim, self.clock(), f"{type(error).__name__}: {error}")
            raise
        if not self.ledger.finish(claim, self.clock(), summary.counts()):
            raise RuntimeError(f"{files.month}: the claim ran out before the load finished")
        LOG.info("%s: %s", files.month, summary)
        return Outcome.DONE

    def load(self, files: MonthFiles) -> LoadSummary:
        """Read both files, build the month's items and write them, timing each part"""
        raw = S3File(FileKind.RAW, files.month)
        counts = raw.partner
        LOG.info("%s: loading %s and %s", files.month, raw.key, counts.key)
        started = time.monotonic()
        trips_csv = self.files.read_text(raw.key, files.raw_etag)
        counts_csv = self.files.read_text(counts.key, files.metrics_etag)
        read = time.monotonic()
        month = build_month(files.month, trips_csv, counts_csv)
        built = time.monotonic()
        if month.departures != month.trips or month.returns != month.trips:
            LOG.warning(
                "%s: %d trips, but the station counts add up to %d departures and %d returns",
                files.month,
                month.trips,
                month.departures,
                month.returns,
            )
        removed = sum(self.writer.replace(part) for part in month.slices())
        return LoadSummary(
            trips=month.trips,
            valid_trips=month.valid_trips,
            days=month.days,
            stations=len(month.stations),
            raw_items=len(month.raw_trips),
            removed_items=removed,
            read_seconds=read - started,
            build_seconds=built - read,
            write_seconds=time.monotonic() - built,
        )


@functools.cache
def default_loader() -> MonthLoader:
    """This Lambda container's loader: made for its first event, reused for the rest"""
    logging.getLogger("bikes").setLevel(logging.INFO)
    return MonthLoader.from_aws()


def handle(event: dict[str, Any], context: Any) -> BatchResponse:
    """The Lambda's entry point, bikes.loader.handler.handle"""
    return default_loader().handle_batch(event)
