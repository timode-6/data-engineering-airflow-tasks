"""Split the Kaggle file into one CSV per month of departure

    python -m bikes.split data/source/archive.zip data/incoming
    python -m bikes.split data/source/archive.zip data/incoming --months 2016-10 2019-06

The source is not sorted by date, so there are two passes: every row goes to a
work file for its month, then each month drops exact duplicates, is sorted
and written as trips_YYYY-MM.csv. Values stay text, unchanged. Each file
appears in one rename, so the DAG never picks up a half-written one.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
import time
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bikes.dataset import DEPARTURE, ENCODING, open_sources, read_chunks
from bikes.files import file_name, month_argument

log = logging.getLogger(__name__)

DEFAULT_CHUNK_ROWS = 250_000


@dataclass(frozen=True)
class SourceStats:
    """What the first pass read from the source"""

    rows: int
    undated: int  # departure is not a timestamp, so the row has no month
    skipped: int  # in a month that was not asked for


@dataclass(frozen=True)
class MonthFile:
    """One finished output file"""

    month: str
    path: Path
    rows: int
    duplicates: int


def months_of(departures: pd.Series) -> pd.Series:
    """The "YYYY-MM" of each departure; NaN where it is not a timestamp"""
    parsed = pd.to_datetime(departures, format="ISO8601", errors="coerce")
    return parsed.dt.strftime("%Y-%m")


def split_by_month(
    source: Path,
    out_dir: Path,
    months: set[str] | None = None,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
) -> tuple[SourceStats, list[MonthFile]]:
    """Write out_dir/trips_YYYY-MM.csv for every month in source, or only for months"""
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".split-", dir=out_dir) as work:
        work_dir = Path(work)
        stats = partition(source, work_dir, months, chunk_rows)
        written = []
        for work_file in sorted(work_dir.glob("*.csv")):
            month_file = finish_month(work_file, out_dir)
            log_month(month_file)
            written.append(month_file)
    return stats, written


def partition(
    source: Path, work_dir: Path, months: set[str] | None, chunk_rows: int
) -> SourceStats:
    """First pass: append every row of source to work_dir/YYYY-MM.csv"""
    rows = undated = skipped = 0
    with ExitStack() as stack:
        for name, handle in open_sources(source, stack):
            log.info("Reading %s", name)
            for chunk in read_chunks(handle, chunk_rows):
                rows += len(chunk)
                month = months_of(chunk[DEPARTURE])
                undated += int(month.isna().sum())
                # groupby leaves out the NaN months, i.e. the undated rows
                for value, group in chunk.groupby(month, sort=False):
                    if months is not None and value not in months:
                        skipped += len(group)
                        continue
                    append_csv(group, work_dir / f"{value}.csv")
    return SourceStats(rows=rows, undated=undated, skipped=skipped)


def append_csv(rows: pd.DataFrame, path: Path) -> None:
    """Append rows to path, writing the header only when the file is new"""
    rows.to_csv(
        path,
        mode="a",
        header=not path.exists(),
        index=False,
        encoding=ENCODING,
        lineterminator="\n",  # not the platform's, so files match on Windows too
    )


def finish_month(work_file: Path, out_dir: Path) -> MonthFile:
    """Second pass for one month: drop exact duplicates, sort, write atomically"""
    rows = pd.read_csv(work_file, dtype=str, keep_default_na=False, encoding=ENCODING)
    unique = rows.drop_duplicates()
    ordered = unique.sort_values(
        DEPARTURE, kind="stable", key=lambda values: pd.to_datetime(values, format="ISO8601")
    )
    month = work_file.stem
    target = out_dir / file_name(month)
    partial = target.with_name(f"{target.name}.part")
    ordered.to_csv(partial, index=False, encoding=ENCODING, lineterminator="\n")
    partial.replace(target)
    return MonthFile(
        month=month, path=target, rows=len(ordered), duplicates=len(rows) - len(unique)
    )


def log_month(month_file: MonthFile) -> None:
    """One line per finished month, e.g. "2019-06    715,130 rows  trips_2019-06.csv" """
    rows = f"{month_file.rows:,}"
    note = f"  ({month_file.duplicates:,} duplicates removed)" if month_file.duplicates else ""
    log.info("%s %10s rows  %s%s", month_file.month, rows, month_file.path.name, note)


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m bikes.split",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("source", type=Path, help="the Kaggle zip, or the CSV inside it")
    parser.add_argument("out_dir", type=Path, help="where the monthly files go")
    parser.add_argument(
        "--months", nargs="+", type=month_argument, metavar="YYYY-MM", help="only these months"
    )
    parser.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS, help="rows per read")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)
    months = set(args.months) if args.months else None

    started = time.monotonic()
    stats, written = split_by_month(args.source, args.out_dir, months, args.chunk_rows)

    rows = sum(month_file.rows for month_file in written)
    duplicates = sum(month_file.duplicates for month_file in written)
    log.info(
        "Done in %.0f s: read %s rows, wrote %s rows to %d files, removed %s duplicates",
        time.monotonic() - started, f"{stats.rows:,}", f"{rows:,}", len(written), f"{duplicates:,}",
    )  # fmt: skip
    if stats.undated:
        log.warning("Left out %s rows whose departure is not a timestamp", f"{stats.undated:,}")
    if stats.skipped:
        log.info("Skipped %s rows from months not asked for", f"{stats.skipped:,}")
    if months and (absent := months - {month_file.month for month_file in written}):
        log.warning("No rows for %s", ", ".join(sorted(absent)))


if __name__ == "__main__":
    main()
