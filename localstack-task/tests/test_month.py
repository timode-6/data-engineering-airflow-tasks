from __future__ import annotations

import pytest

from bikes.loader.month import build_month
from bikes.resources import DAILY_METRICS, MONTHLY_METRICS, STATION_METRICS, TRIPS
from helpers import counts_text, source_text, trip

ROWS = [
    trip("2019-06-01 08:00:00", departure_name="Kamppi (M)", return_name="Kuusitie"),
    trip("2019-06-01 09:00:00", departure_name="Kamppi (M)", return_name="Kamppi (M)"),
    trip(
        "2019-06-30 23:59:00", departure_name="Kuusitie", return_name="Kamppi (M)", distance="0.0"
    ),
]


def test_a_month_gives_items_for_all_four_tables():
    month = build_month("2019-06", source_text(ROWS), counts_text(ROWS))

    assert (month.trips, month.valid_trips, month.days) == (3, 2, 2)
    assert [(i["trip_date"], i["station_part"]) for i in month.raw_trips] == [
        ("2019-06-01", "Kamppi (M)#0"),
        ("2019-06-30", "Kuusitie#0"),
    ]
    assert [item["trip_date"] for item in month.daily] == ["2019-06-01", "2019-06-30"]
    assert [item["trip_month"] for item in month.monthly] == ["2019-06"]
    assert {item["station_name"]: item["departures"] for item in month.stations} == {
        "Kamppi (M)": 2,
        "Kuusitie": 1,
    }
    assert month.departures == month.returns == 3


def test_each_table_gets_the_partitions_the_month_owns():
    month = build_month("2019-06", source_text(ROWS), counts_text(ROWS))

    slices = {part.spec: part for part in month.slices()}

    assert [part.spec for part in month.slices()][0] == TRIPS  # the raw trips go first
    assert slices[TRIPS].partitions[0] == "2019-06-01"
    assert slices[TRIPS].partitions[-1] == "2019-06-30"
    assert len(slices[TRIPS].partitions) == 30  # every day, not only the days with trips
    assert slices[DAILY_METRICS].partitions == slices[STATION_METRICS].partitions == ("2019-06",)
    assert slices[MONTHLY_METRICS].partitions == ()  # its year holds other months too


def test_february_of_a_leap_year_has_29_days():
    rows = [trip("2020-02-29 12:00:00")]

    month = build_month("2020-02", source_text(rows), counts_text(rows))

    assert month.days_of_month()[-1] == "2020-02-29"


def test_a_file_with_another_months_trips_is_refused():
    rows = [trip("2019-06-30 23:59:00"), trip("2019-07-01 00:01:00")]

    with pytest.raises(ValueError, match="the trips file for 2019-06 has 2019-06, 2019-07"):
        build_month("2019-06", source_text(rows), counts_text(rows))


def test_station_counts_that_disagree_are_reported_not_hidden():
    counts = "station_name,departures,returns\nKamppi (M),1,0\n"

    month = build_month("2019-06", source_text(ROWS), counts)

    assert (month.trips, month.departures, month.returns) == (3, 1, 0)
