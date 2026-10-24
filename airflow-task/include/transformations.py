"""Row-level transformations for the TikTok Google Play reviews pipeline"""

from __future__ import annotations

import re

import pandas as pd

# The source CSV calls the review timestamp "at". The task specification refers to
# "created_date", so the column is renamed once, here, and every later stage —
# including the MongoDB documents — uses the specification's name.
SOURCE_DATE_COLUMN = "at"
DATE_COLUMN = "created_date"

NULL_PLACEHOLDER = "-"

# Values that are "null" in the task's sense: a genuinely missing field, or a
# null word as the whole value, matched case-insensitively. The source has
# 89,216 empty reviewCreatedVersion, 306,938 empty replyContent/repliedAt, and
# 18 reviews whose entire content is a null word: "None" x12, "N/A" x4, "Null" x2.
LITERAL_NULLS = {"null", "none", "nan", "n/a", "na", ""}

# Deliberately a blocklist of symbol ranges rather than a whitelist of ASCII: the
# reviews include Cyrillic, Arabic and CJK text, and "leave only text and
# punctuation" means keep those, not keep English. A whitelist of [A-Za-z0-9]
# would silently delete every non-Latin review.
_SYMBOL_RANGES = (
    "\U0001f000-\U0001faff"  # emoji, pictographs, supplemental symbols, flags
    "\u2190-\u21ff"  # arrows
    "\u2300-\u23ff"  # misc technical (⌚, ⏰)
    "\u2460-\u24ff"  # enclosed alphanumerics
    "\u25a0-\u27bf"  # geometric shapes, misc symbols, dingbats
    "\u2b00-\u2bff"  # misc symbols and arrows
    "\ue000-\uf8ff"  # private use area
    "\ufe00-\ufe0f"  # variation selectors
    "\u200b-\u200f"  # zero-width space/joiner, directional marks
    "\u20d0-\u20ff"  # combining diacritics for symbols (keycap enclosures)
    "\u2122\u00a9\u00ae"  # ™ © ®
)
SYMBOL_PATTERN = re.compile(f"[{_SYMBOL_RANGES}]")

# Runs of whitespace left behind once symbols are removed.
WHITESPACE_PATTERN = re.compile(r"\s{2,}")


def _read(path: str) -> pd.DataFrame:
    """Read the staged CSV without pandas inventing NaN for us"""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])


def replace_nulls(source: str, destination: str) -> dict[str, int]:
    """Stage 1 - every null becomes "-", and "at" becomes "created_date" """
    frame = _read(source)
    frame = frame.rename(columns={SOURCE_DATE_COLUMN: DATE_COLUMN})

    empty = 0
    literal = 0
    for column in frame.columns:
        normalised = frame[column].str.strip().str.lower()
        blank = normalised == ""
        named = normalised.isin(LITERAL_NULLS - {""})

        empty += int(blank.sum())
        literal += int(named.sum())
        frame.loc[blank | named, column] = NULL_PLACEHOLDER

    frame.to_csv(destination, index=False)
    return {
        "rows": len(frame),
        "empty_fields_filled": empty,
        "literal_nulls_replaced": literal,
    }


def sort_by_created_date(source: str, destination: str) -> dict[str, int]:
    """Stage 2 - sort ascending by created_date"""
    frame = _read(source)
    parsed = pd.to_datetime(frame[DATE_COLUMN], errors="coerce", format="mixed")

    unparseable = int(parsed.isna().sum())
    frame = frame.assign(_sort_key=parsed).sort_values(
        "_sort_key", ascending=True, na_position="last", kind="stable"
    )
    frame = frame.drop(columns="_sort_key")

    frame.to_csv(destination, index=False)
    return {"rows": len(frame), "unparseable_dates": unparseable}


def clean_content(source: str, destination: str) -> dict[str, int]:
    """Stage 3 — strip emoji and symbols from content, keeping text and punctuation"""
    frame = _read(source)
    original = frame["content"]

    cleaned = (
        original.str.replace(SYMBOL_PATTERN, "", regex=True)
        .str.replace(WHITESPACE_PATTERN, " ", regex=True)
        .str.strip()
    )

    touched = int((cleaned != original).sum())
    emptied = int((cleaned == "").sum())
    cleaned = cleaned.replace("", NULL_PLACEHOLDER)

    frame["content"] = cleaned
    frame.to_csv(destination, index=False)
    return {"rows": len(frame), "content_changed": touched, "content_emptied": emptied}
