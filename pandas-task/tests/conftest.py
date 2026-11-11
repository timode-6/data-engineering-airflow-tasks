"""Fixtures shared by the tests."""

from __future__ import annotations

import csv
from collections.abc import Callable, Iterable
from pathlib import Path

import pytest


@pytest.fixture
def write_journal(tmp_path: Path) -> Callable[..., Path]:
    """Write rows to a CSV journal the way the app does: no header, quoted where needed."""

    def write(rows: Iterable[list[str]], name: str = "journal.csv") -> Path:
        path = tmp_path / name
        with path.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerows(rows)
        return path

    return write
