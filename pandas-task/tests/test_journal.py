from __future__ import annotations

import pandas as pd

from helpers import START, make_row
from log_alerts.journal import COLUMNS, read_journal


def read_all(path, chunk_rows=100_000):
    chunks = list(read_journal(path, chunk_rows))
    frames = [chunk.events for chunk in chunks]
    events = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return events, sum(chunk.rejected for chunk in chunks), len(chunks)


def test_reads_every_column_and_converts_date(write_journal):
    path = write_journal([make_row(date=START), make_row(date=START + 1.5, severity="Success")])

    events, rejected, _ = read_all(path)

    assert list(events.columns) == list(COLUMNS)
    assert rejected == 0
    assert events["date"].tolist() == [START, START + 1.5]
    assert events["severity"].tolist() == ["Error", "Success"]


def test_quoted_message_with_comma_and_line_breaks_stays_one_row(write_journal):
    message = 'We are running "Programmatic Flow", you need to active gCom.\n HTTP/1.1 400\n{}'
    path = write_journal([make_row(error_message=message), make_row()])

    events, rejected, _ = read_all(path)

    assert rejected == 0
    assert events["error_message"].tolist()[0] == message
    assert events["bundle_id"].nunique() == 1


def test_line_with_an_extra_field_is_dropped_not_shifted(write_journal):
    good = make_row(date=START)
    shifted = good[:1] + ["could not receive", " ip address"] + good[2:]  # an unquoted comma
    path = write_journal([good, shifted, make_row(date=START + 2)])

    events, rejected, _ = read_all(path)

    assert rejected == 1
    assert events["date"].tolist() == [START, START + 2]
    assert set(events["severity"]) == {"Error"}


def test_rows_without_a_usable_date_or_severity_are_rejected(write_journal):
    column_numbers = [str(i) for i in range(len(COLUMNS))]  # the dataset's first line
    path = write_journal(
        [
            column_numbers,
            make_row(date="not a number"),
            make_row(date=""),
            make_row(date=4_000_000_000),  # the year 2096
            make_row(severity=""),
            make_row(date=START),
            make_row()[:5],  # a cut-off line
        ]
    )

    events, rejected, _ = read_all(path)

    assert rejected == 6
    assert events["date"].tolist() == [START]


def test_large_journal_is_read_in_chunks(write_journal):
    path = write_journal([make_row(date=START + i) for i in range(5)])

    events, rejected, chunk_count = read_all(path, chunk_rows=2)

    assert chunk_count == 3
    assert rejected == 0
    assert events["date"].tolist() == [START + i for i in range(5)]


def test_empty_journal_gives_no_rows(write_journal):
    path = write_journal([])

    events, rejected, _ = read_all(path)

    assert events.empty
    assert rejected == 0
