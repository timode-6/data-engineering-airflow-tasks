"""The CSV export, on tables filled the way the Lambda fills them"""

from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bikes import export as export_module
from bikes.config import AwsSettings
from bikes.export import export
from bikes.loader.month import build_month
from bikes.loader.stores import Ledger, MonthFiles, Writer
from bikes.resources import LOADS, TABLES
from fakes import REGION, FakeDynamoDB, FakeTables
from helpers import counts_text, source_text, trip

JUNE = [
    trip(
        "2019-06-01 08:00:00",
        distance="2000.0",
        duration="600.0",
        temperature="15.0",
        departure_name="Kamppi (M)",
        return_name="Töölöntori",
    ),
    trip(
        "2019-06-01 09:00:00",
        distance="4000.0",
        duration="1200.0",
        temperature="17.0",
        departure_name="Töölöntori",
        return_name="Kamppi (M)",
    ),
    trip("2019-06-02 08:00:00", distance="0.0", duration="600.0", temperature="12.0"),
]
JULY = [trip("2019-07-01 12:00:00", distance="3000.0", duration="900.0", temperature="20.0")]


def loaded(months: dict[str, list[dict[str, str]]]) -> FakeDynamoDB:
    """Tables holding these months, written by the Lambda's own code."""
    dynamodb = FakeDynamoDB(list(TABLES))
    writer = Writer(lambda: dynamodb)
    for month, rows in months.items():
        for part in build_month(month, source_text(rows), counts_text(rows)).slices():
            writer.replace(part)
    return dynamodb


def read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture
def files(tmp_path):
    written = export(loaded({"2019-07": JULY, "2019-06": JUNE}), tmp_path, report=lambda _: None)
    return {path.stem: path for path in written}


def test_each_metrics_table_becomes_a_file_named_after_it(files):
    assert sorted(files) == ["bike_daily_metrics", "bike_monthly_metrics", "bike_station_metrics"]


def test_days_come_in_order_in_the_units_the_charts_show(files):
    first, second, third = read(files["bike_daily_metrics"])

    assert list(first) == [
        "Date",
        "Trips",
        "Valid trips",
        "Avg distance (km)",
        "Avg duration (min)",
        "Avg speed (km/h)",
        "Avg temperature (degC)",
        "Temperature hours",
    ]
    assert [row["Date"] for row in (first, second, third)] == [
        "2019-06-01",
        "2019-06-02",
        "2019-07-01",
    ]
    assert first == {
        "Date": "2019-06-01",
        "Trips": "2",
        "Valid trips": "2",
        "Avg distance (km)": "3.0",
        "Avg duration (min)": "15.0",
        "Avg speed (km/h)": "12.0",
        "Avg temperature (degC)": "16.0",
        "Temperature hours": "2",
    }


def test_a_day_without_valid_trips_has_empty_averages_but_its_temperature(files):
    day = read(files["bike_daily_metrics"])[1]

    assert (day["Trips"], day["Valid trips"]) == ("1", "0")
    assert day["Avg distance (km)"] == day["Avg duration (min)"] == day["Avg speed (km/h)"] == ""
    assert day["Avg temperature (degC)"] == "12.0"


def test_months_are_dates_and_average_their_trips(files):
    june, july = read(files["bike_monthly_metrics"])

    assert list(june)[:4] == ["Month", "Days with trips", "Trips", "Valid trips"]
    assert (june["Month"], june["Days with trips"], june["Trips"]) == ("2019-06-01", "2", "3")
    assert june["Avg distance (km)"] == "3.0"
    assert june["Avg temperature (degC)"] == "14.67"
    assert july["Month"] == "2019-07-01"


def test_stations_by_month_keep_their_names(files):
    rows = read(files["bike_station_metrics"])

    june = [(row["Station"], row["Departures"], row["Returns"]) for row in rows[:4]]
    assert june == [
        ("Esterinportti", "0", "1"),
        ("Kamppi (M)", "1", "1"),
        ("Kuusitie", "1", "0"),
        ("Töölöntori", "1", "1"),
    ]
    assert {row["Month"] for row in rows} == {"2019-06-01", "2019-07-01"}
    assert files["bike_station_metrics"].read_bytes().startswith(b"\xef\xbb\xbf")  # a UTF-8 BOM


def test_empty_tables_give_files_with_headers_only(tmp_path):
    written = export(FakeDynamoDB(list(TABLES)), tmp_path, report=lambda _: None)

    assert [read(path) for path in written] == [[], [], []]
    assert written[0].read_text(encoding="utf-8-sig").startswith("Date,Trips,")


# --- command line


@pytest.fixture
def aws_fakes(monkeypatch):
    dynamodb = loaded({"2019-06": JUNE})
    client = FakeTables()
    settings = AwsSettings(endpoint_url="http://localhost:4566", region=REGION)
    monkeypatch.setattr(export_module.AwsSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(export_module.aws, "client", lambda service: client)
    monkeypatch.setattr(export_module.aws, "resource", lambda service: dynamodb)
    return client, dynamodb


NOON = datetime(2026, 11, 4, 9, 0, tzinfo=UTC)


def test_the_command_says_which_months_are_not_loaded(aws_fakes, tmp_path, capsys):
    client, dynamodb = aws_fakes
    client.tables = {spec.name: [] for spec in TABLES}
    ledger = Ledger(dynamodb.Table(LOADS.name))
    june = ledger.claim(MonthFiles("2019-06", "r", "m"), NOON, timedelta(minutes=5))
    ledger.finish(june, NOON, {})
    july = ledger.claim(MonthFiles("2019-07", "r", "m"), NOON, timedelta(minutes=5))
    ledger.fail(july, NOON, "ValueError: unexpected header")

    export_module.main(["--out", str(tmp_path)])

    out = capsys.readouterr().out
    assert out.startswith("Not loaded: 2019-07.")
    assert f"{tmp_path / 'bike_daily_metrics.csv'}: 2 rows, 2019-06-01 to 2019-06-02" in out


def test_with_nothing_loaded_the_command_keeps_the_files_it_made_before(aws_fakes, tmp_path):
    client, _ = aws_fakes
    client.tables = {spec.name: [] for spec in TABLES}
    earlier = tmp_path / "bike_daily_metrics.csv"
    earlier.write_text("Date,Trips\n2019-06-01,2\n")

    with pytest.raises(SystemExit, match="Nothing is loaded yet"):
        export_module.main(["--out", str(tmp_path)])
    assert earlier.read_text() == "Date,Trips\n2019-06-01,2\n"


def test_the_command_needs_the_tables(aws_fakes, tmp_path):
    with pytest.raises(SystemExit, match="Missing: bike_daily_metrics, bike_loads"):
        export_module.main(["--out", str(tmp_path)])
