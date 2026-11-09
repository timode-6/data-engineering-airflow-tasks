"""The dataset as Kaggle ships it: its columns, and how to read it unchanged"""

from __future__ import annotations

import zipfile
from collections.abc import Iterable, Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import IO

import pandas as pd

DEPARTURE = "departure"
RETURN = "return"
DEPARTURE_NAME = "departure_name"
RETURN_NAME = "return_name"
DISTANCE = "distance (m)"
DURATION = "duration (sec.)"
TEMPERATURE = "Air temperature (degC)"

COLUMNS = (
    DEPARTURE, RETURN, "departure_id", DEPARTURE_NAME, "return_id", RETURN_NAME,
    DISTANCE, DURATION, "avg_speed (km/h)",
    "departure_latitude", "departure_longitude", "return_latitude", "return_longitude",
    TEMPERATURE,
)  # fmt: skip

ENCODING = "utf-8"


def open_sources(path: Path, stack: ExitStack) -> Iterator[tuple[str, IO[bytes]]]:
    """Yield (name, binary handle) for a CSV file, or for each CSV inside a zip"""
    if not zipfile.is_zipfile(path):
        yield path.name, stack.enter_context(path.open("rb"))
        return
    archive = stack.enter_context(zipfile.ZipFile(path))
    members = sorted(name for name in archive.namelist() if name.lower().endswith(".csv"))
    if not members:
        raise ValueError(f"{path} has no CSV inside: {archive.namelist()}")
    for member in members:
        yield member, stack.enter_context(archive.open(member))


def read_chunks(handle: IO[bytes], chunk_rows: int) -> Iterator[pd.DataFrame]:
    """Read rows as text, exactly as written, chunk_rows at a time"""
    reader = pd.read_csv(
        handle,
        dtype=str,
        keep_default_na=False,
        chunksize=chunk_rows,
        encoding=f"{ENCODING}-sig",  # tolerates a byte-order mark
    )
    for chunk in reader:
        check_columns(chunk.columns)
        yield chunk


def check_columns(columns: Iterable[str]) -> None:
    """Raise ValueError unless the header is exactly the dataset's, in order"""
    found = list(columns)
    if found != list(COLUMNS):
        raise ValueError(f"unexpected header {found}; expected {list(COLUMNS)}")
