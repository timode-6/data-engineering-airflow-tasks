"""The monthly files on disk: their names, and how the DAG picks them up

    data/incoming/trips_2019-06.csv            from the split, waiting for the DAG
    data/metrics/station_counts_2019-06.csv    from the Spark job
    data/processed/                            both, once uploaded
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from bikes.resources import FileKind

MONTH = re.compile(r"\d{4}-(?:0[1-9]|1[0-2])")  # 2019-06
NAME = re.compile(rf"trips_(?P<month>{MONTH.pattern})\.csv")


def month_argument(value: str) -> str:
    """argparse type for a month, YYYY-MM"""
    if not MONTH.fullmatch(value):
        raise argparse.ArgumentTypeError(f"{value!r} is not a month like 2019-06")
    return value


def file_name(month: str, kind: FileKind = FileKind.RAW) -> str:
    """trips_2019-06.csv, or station_counts_2019-06.csv for the metrics file"""
    return f"{kind.stem}_{month}.csv"


def month_of(path: Path | str) -> str | None:
    """The month in a trips file's name, e.g. 2019-06; None for any other name"""
    match = NAME.fullmatch(Path(path).name)
    return match["month"] if match else None


def waiting_files(folder: Path) -> list[Path]:
    """The trips files in folder, oldest month first; none if it doesn't exist"""
    if not folder.is_dir():
        return []
    return sorted(path for path in folder.iterdir() if path.is_file() and month_of(path))


def archive(path: Path, processed: Path) -> Path:
    """Move path into processed, replacing an older copy; returns the new path"""
    target = processed / path.name
    if not path.exists() and target.exists():
        return target
    processed.mkdir(parents=True, exist_ok=True)
    return path.replace(target)
