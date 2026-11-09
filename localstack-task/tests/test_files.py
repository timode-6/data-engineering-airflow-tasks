from __future__ import annotations

import argparse

import pytest

from bikes.files import archive, file_name, month_argument, month_of, waiting_files
from bikes.resources import FileKind


def test_a_month_names_its_files_and_back():
    assert file_name("2019-06") == "trips_2019-06.csv"
    assert file_name("2019-06", FileKind.METRICS) == "station_counts_2019-06.csv"
    assert month_of("data/incoming/trips_2019-06.csv") == "2019-06"


@pytest.mark.parametrize(
    "name",
    [
        "trips_2019-06.csv.part",
        "trips_2019-13.csv",
        "trips_2019-6.csv",
        "notes.txt",
        "x_trips_2019-06.csv",
    ],
)
def test_other_names_have_no_month(name):
    assert month_of(name) is None


def test_waiting_files_are_the_monthly_ones_oldest_first(tmp_path):
    for name in ["trips_2020-03.csv", "trips_2016-05.csv", "trips_2019-06.csv.part", ".gitkeep"]:
        (tmp_path / name).write_text("x", encoding="utf-8")
    (tmp_path / ".split-abc").mkdir()  # the split's work folder

    assert [path.name for path in waiting_files(tmp_path)] == [
        "trips_2016-05.csv",
        "trips_2020-03.csv",
    ]


def test_a_missing_folder_has_nothing_waiting(tmp_path):
    assert waiting_files(tmp_path / "incoming") == []


def test_archive_moves_the_file_and_replaces_an_older_copy(tmp_path):
    incoming, processed = tmp_path / "incoming", tmp_path / "processed"
    incoming.mkdir()
    source = incoming / "trips_2019-06.csv"
    source.write_text("new", encoding="utf-8")
    processed.mkdir()
    (processed / "trips_2019-06.csv").write_text("old", encoding="utf-8")

    moved = archive(source, processed)

    assert moved == processed / "trips_2019-06.csv"
    assert moved.read_text(encoding="utf-8") == "new"
    assert not source.exists()


def test_archive_creates_the_folder(tmp_path):
    source = tmp_path / "trips_2019-06.csv"
    source.write_text("x", encoding="utf-8")

    assert archive(source, tmp_path / "processed").exists()


def test_archiving_again_after_it_worked_is_fine(tmp_path):
    source = tmp_path / "trips_2019-06.csv"
    source.write_text("x", encoding="utf-8")
    first = archive(source, tmp_path / "processed")

    assert archive(source, tmp_path / "processed") == first


def test_archiving_a_file_that_was_never_there_fails(tmp_path):
    with pytest.raises(FileNotFoundError):
        archive(tmp_path / "trips_2019-06.csv", tmp_path / "processed")


@pytest.mark.parametrize("value", ["2016-05", "2019-12"])
def test_a_month_argument_is_a_year_and_a_month(value):
    assert month_argument(value) == value


@pytest.mark.parametrize("value", ["2019-13", "2019-6", "19-06", "2019-06-01", "June"])
def test_anything_else_is_not_a_month(value):
    with pytest.raises(argparse.ArgumentTypeError, match="not a month like 2019-06"):
        month_argument(value)
