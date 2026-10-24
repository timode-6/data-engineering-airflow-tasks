"""Tests for the three transformations.

No Airflow needed — the functions are plain file-in, file-out:

    pip install pandas pytest
    python -m pytest tests/ -v
"""

from __future__ import annotations

import pandas as pd
import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from transformations import (  # noqa: E402
    NULL_PLACEHOLDER,
    clean_content,
    replace_nulls,
    sort_by_created_date,
)


def read(path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])


@pytest.fixture
def source(tmp_path):
    """A small file with one of every awkward case."""
    path = tmp_path / "source.csv"
    path.write_text(
        "reviewId,content,score,reviewCreatedVersion,at\n"
        # empty field, and an out-of-order date
        "r1,Good app,5,,2022-03-02 10:00:00\n"
        # the literal text "null" as content
        "r2,null,3,23.8.4,2022-01-15 09:30:00\n"
        # emoji mixed with text
        "r3,Love it 😍🔥,5,23.9.5,2022-02-20 12:00:00\n"
        # nothing but emoji
        "r4,😎😎,4,22.2.5,2022-01-01 08:00:00\n"
        # non-Latin text, must survive
        "r5,очень хорошо,5,23.8.4,2022-04-01 23:59:59\n"
        # quoted comma plus trailing whitespace from a stripped symbol
        'r6,"Nice, but slow ⌚",2,,2022-02-01 00:00:01\n',
        encoding="utf-8",
    )
    return path


def test_replace_nulls_fills_empty_and_literal(source, tmp_path):
    out = tmp_path / "out.csv"
    stats = replace_nulls(str(source), str(out))
    frame = read(out)

    assert stats["rows"] == 6
    # two empty reviewCreatedVersion fields
    assert stats["empty_fields_filled"] == 2
    # r2's content is the string "null"
    assert stats["literal_nulls_replaced"] == 1

    assert frame.loc[0, "reviewCreatedVersion"] == NULL_PLACEHOLDER
    assert frame.loc[1, "content"] == NULL_PLACEHOLDER
    # untouched values stay exactly as they were
    assert frame.loc[0, "content"] == "Good app"


def test_replace_nulls_renames_the_date_column(source, tmp_path):
    out = tmp_path / "out.csv"
    replace_nulls(str(source), str(out))
    frame = read(out)

    assert "created_date" in frame.columns
    assert "at" not in frame.columns


def test_sort_is_chronological_not_lexicographic(source, tmp_path):
    staged = tmp_path / "staged.csv"
    out = tmp_path / "out.csv"
    replace_nulls(str(source), str(staged))
    stats = sort_by_created_date(str(staged), str(out))
    frame = read(out)

    assert stats["rows"] == 6
    assert stats["unparseable_dates"] == 0
    assert frame["reviewId"].tolist() == ["r4", "r2", "r6", "r3", "r1", "r5"]

    parsed = pd.to_datetime(frame["created_date"], format="mixed")
    assert parsed.is_monotonic_increasing


def test_sort_keeps_unparseable_dates_rather_than_dropping_them(tmp_path):
    source = tmp_path / "bad.csv"
    source.write_text(
        "reviewId,content,created_date\n"
        "r1,fine,2022-01-02 00:00:00\n"
        "r2,broken,not a date\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.csv"
    stats = sort_by_created_date(str(source), str(out))
    frame = read(out)

    assert stats["unparseable_dates"] == 1
    assert len(frame) == 2
    # the unparseable row sorts last, it is not discarded
    assert frame["reviewId"].tolist() == ["r1", "r2"]


def test_clean_content_strips_symbols_and_keeps_text(source, tmp_path):
    staged = tmp_path / "staged.csv"
    out = tmp_path / "out.csv"
    replace_nulls(str(source), str(staged))
    stats = clean_content(str(staged), str(out))
    frame = read(out).set_index("reviewId")

    assert frame.loc["r3", "content"] == "Love it"
    # an emoji-only review has nothing left, so it becomes the placeholder
    assert frame.loc["r4", "content"] == NULL_PLACEHOLDER
    assert stats["content_emptied"] == 1
    # the symbol goes, the comma and the words stay, no double space is left
    assert frame.loc["r6", "content"] == "Nice, but slow"


def test_clean_content_preserves_non_latin_text(source, tmp_path):
    staged = tmp_path / "staged.csv"
    out = tmp_path / "out.csv"
    replace_nulls(str(source), str(staged))
    clean_content(str(staged), str(out))
    frame = read(out).set_index("reviewId")

    # the whole point of blocklisting symbol ranges instead of whitelisting ASCII
    assert frame.loc["r5", "content"] == "очень хорошо"


def test_no_rows_are_lost_across_the_three_stages(source, tmp_path):
    a, b, c = (tmp_path / n for n in ("a.csv", "b.csv", "c.csv"))

    assert replace_nulls(str(source), str(a))["rows"] == 6
    assert sort_by_created_date(str(a), str(b))["rows"] == 6
    assert clean_content(str(b), str(c))["rows"] == 6
