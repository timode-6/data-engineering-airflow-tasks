from __future__ import annotations

import logging
import zipfile

import pandas as pd
import pytest

from bikes.dataset import COLUMNS
from bikes.split import main, split_by_month
from helpers import read_rows, trip, write_source


def test_each_month_gets_one_file_sorted_by_departure(tmp_path):
    # date-sorted blocks, with 2020 first
    source = write_source(tmp_path, [
        trip("2020-03-23 07:00:00"),
        trip("2020-03-23 08:00:00"),
        trip("2016-05-02 09:31:00", departure_id="A10"),
        trip("2020-03-23 06:00:00"),
        trip("2016-06-01 10:00:00", departure_id="A11"),
    ])

    stats, written = split_by_month(source, tmp_path / "incoming")

    assert [f.path.name for f in written] == [
        "trips_2016-05.csv", "trips_2016-06.csv", "trips_2020-03.csv",
    ]  # fmt: skip
    assert [row["departure"] for row in read_rows(written[2].path)] == [
        "2020-03-23 06:00:00", "2020-03-23 07:00:00", "2020-03-23 08:00:00",
    ]  # fmt: skip
    assert stats.rows == 5
    assert sum(f.rows for f in written) == 5


def test_values_are_copied_exactly_as_written(tmp_path):
    original = trip(
        "2020-03-23 06:09:44",
        return_id="111.0",  # not 111
        departure_name='Kamppi, "Kauppakeskus"',
        return_name="Länsisatamankatu",
        speed="",  # empty, as when duration is 0
        temperature="",
    )
    source = write_source(tmp_path, [original])

    _, [month_file] = split_by_month(source, tmp_path / "incoming")

    assert read_rows(month_file.path) == [original]
    assert month_file.path.read_text(encoding="utf-8").splitlines()[0] == ",".join(COLUMNS)


def test_exact_duplicates_are_removed_and_counted(tmp_path):
    twice = trip("2020-03-23 06:09:44")
    almost = trip("2020-03-23 06:09:44", duration="402.0")  # one field differs: kept
    source = write_source(tmp_path, [twice, almost, twice])

    _, [month_file] = split_by_month(source, tmp_path / "incoming")

    assert (month_file.rows, month_file.duplicates) == (2, 1)
    assert read_rows(month_file.path) == [twice, almost]  # source order for ties


def test_reads_the_csv_inside_the_kaggle_zip(tmp_path):
    csv_path = write_source(tmp_path, [trip("2020-03-23 06:09:44")])
    archive = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as handle:
        handle.write(csv_path, "database.csv")

    _, written = split_by_month(archive, tmp_path / "incoming")

    assert [f.month for f in written] == ["2020-03"]


def test_a_byte_order_mark_is_not_part_of_the_header(tmp_path):
    source = write_source(tmp_path, [trip("2020-03-23 06:09:44")], encoding="utf-8-sig")

    _, written = split_by_month(source, tmp_path / "incoming")

    assert written[0].path.read_bytes().startswith(b"departure,")


def test_months_option_writes_only_those_months(tmp_path):
    source = write_source(tmp_path, [trip("2016-05-02 09:31:00"), trip("2020-03-23 06:09:44")])

    stats, written = split_by_month(source, tmp_path / "incoming", months={"2016-05"})

    assert [f.month for f in written] == ["2016-05"]
    assert stats.skipped == 1
    assert not (tmp_path / "incoming" / "trips_2020-03.csv").exists()


def test_rows_without_a_timestamp_are_counted_and_left_out(tmp_path):
    source = write_source(tmp_path, [trip("2020-03-23 06:09:44"), trip("not a date")])

    stats, [month_file] = split_by_month(source, tmp_path / "incoming")

    assert stats.undated == 1
    assert month_file.rows == 1


def test_an_older_file_is_replaced_and_nothing_else_is_left(tmp_path):
    out = tmp_path / "incoming"
    out.mkdir()
    (out / "trips_2020-03.csv").write_text("old\n", encoding="utf-8")
    source = write_source(tmp_path, [trip("2020-03-23 06:09:44")])

    split_by_month(source, out)

    assert [p.name for p in out.iterdir()] == ["trips_2020-03.csv"]  # no .part, no work dir
    assert len(read_rows(out / "trips_2020-03.csv")) == 1


def test_a_different_header_is_refused(tmp_path):
    source = tmp_path / "other.csv"
    source.write_text("departure,return\n2020-03-23 06:09:44,2020-03-23 06:16:26\n")

    with pytest.raises(ValueError, match="unexpected header"):
        split_by_month(source, tmp_path / "incoming")


def test_a_broken_line_stops_the_split_before_anything_is_written(tmp_path):
    source = write_source(tmp_path, [trip("2020-03-23 06:09:44")])
    with source.open("a", encoding="utf-8") as handle:
        handle.write(",".join(trip("2020-03-23 06:10:00").values()) + ",extra\n")

    with pytest.raises(pd.errors.ParserError):
        split_by_month(source, tmp_path / "incoming")

    assert list((tmp_path / "incoming").iterdir()) == []


def test_command_line_reports_each_month_and_a_total(tmp_path, caplog):
    rows = [trip("2016-05-02 09:31:00"), trip("2020-03-23 06:09:44"), trip("2020-03-23 06:09:44")]
    source = write_source(tmp_path, rows)

    with caplog.at_level(logging.INFO):
        main([str(source), str(tmp_path / "incoming"), "--months", "2020-03", "2017-04"])

    assert "trips_2020-03.csv  (1 duplicates removed)" in caplog.text
    assert "wrote 1 rows to 1 files, removed 1 duplicates" in caplog.text
    assert "Skipped 1 rows from months not asked for" in caplog.text
    assert "No rows for 2017-04" in caplog.text


@pytest.mark.parametrize("month", ["2019-13", "2019-6", "June"])
def test_command_line_rejects_a_malformed_month(tmp_path, month):
    with pytest.raises(SystemExit):
        main([str(tmp_path / "archive.zip"), str(tmp_path / "out"), "--months", month])
