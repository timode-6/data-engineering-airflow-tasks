"""The Lambda's pure parts: reading a month, the figures, the DynamoDB items"""

from __future__ import annotations

import gzip
import io
import math
from decimal import Decimal

import pandas as pd
import pytest

from bikes.loader.items import daily_items, monthly_items, raw_trip_items, station_items
from bikes.loader.metrics import DAY, MONTH, days_per_month, period_metrics, with_validity
from bikes.loader.trips import parse_trips
from helpers import trip, write_source


def month_text(tmp_path, rows) -> str:
    return write_source(tmp_path, rows).read_text(encoding="utf-8")


def ride(departure: str, distance="2000.0", duration="600.0", temperature="15.0", station="A"):
    return trip(
        departure,
        distance=distance,
        duration=duration,
        temperature=temperature,
        departure_name=station,
    )


# --- trips.py


def test_lines_and_typed_columns_stay_aligned(tmp_path):
    rows = [ride("2019-06-01 08:00:00", station="Kamppi, M"), ride("2019-06-01 09:00:00")]
    text = month_text(tmp_path, rows)

    trips = parse_trips(text)

    assert trips.lines == text.splitlines()[1:]
    assert list(trips.frame["station"]) == ["Kamppi, M", "A"]
    assert trips.frame["departure"].iloc[1] == pd.Timestamp("2019-06-01 09:00:00")
    assert trips.frame["distance_m"].tolist() == [2000.0, 2000.0]


def test_windows_line_endings_are_fine(tmp_path):
    text = month_text(tmp_path, [ride("2019-06-01 08:00:00")]).replace("\n", "\r\n")

    assert len(parse_trips(text).lines) == 1


def test_lines_pandas_would_not_count_are_caught(tmp_path):
    text = month_text(tmp_path, [ride("2019-06-01 08:00:00")]) + "\n\n"  # blank lines inside
    text += month_text(tmp_path, [ride("2019-06-01 09:00:00")]).split("\n", 1)[1]

    with pytest.raises(ValueError, match="lines but pandas read"):
        parse_trips(text)


def test_another_header_is_refused():
    with pytest.raises(ValueError, match="unexpected header"):
        parse_trips("a,b\n1,2\n")


# --- metrics.py


@pytest.mark.parametrize(
    ("distance", "duration", "valid"),
    [
        ("2000.0", "600.0", True),  # 12 km/h
        ("10.0", "10.0", True),  # the shortest that counts
        ("9.0", "600.0", False),  # under 10 m
        ("0.0", "963.0", False),  # a dead odometer
        ("2000.0", "9.0", False),  # under 10 s
        ("2000.0", "0.0", False),  # no duration, no speed
        ("50000.0", str(5 * 3600 + 1), False),  # longer than an HSL ride may be
        ("5001.0", "600.0", False),  # 30.006 km/h
        ("5000.0", "600.0", True),  # exactly 30 km/h
        ("-4292467.0", "600.0", False),
        ("", "600.0", False),  # missing
    ],
)
def test_what_counts_as_a_ride(tmp_path, distance, duration, valid):
    trips = parse_trips(month_text(tmp_path, [ride("2019-06-01 08:00:00", distance, duration)]))

    assert bool(with_validity(trips.frame)["valid"].iloc[0]) is valid


def test_speed_is_kmh_from_distance_and_duration(tmp_path):
    trips = parse_trips(month_text(tmp_path, [ride("2019-06-01 08:00:00", "1747.0", "401.0")]))

    assert with_validity(trips.frame)["speed_kmh"].iloc[0] == pytest.approx(15.684, abs=0.001)


def frame_of(tmp_path, rows) -> pd.DataFrame:
    return with_validity(parse_trips(month_text(tmp_path, rows)).frame)


def test_averages_cover_valid_trips_and_counts_cover_all(tmp_path):
    frame = frame_of(
        tmp_path,
        [
            ride("2019-06-01 08:00:00", "1000.0", "300.0"),  # 12 km/h
            ride("2019-06-01 09:00:00", "3000.0", "900.0"),  # 12 km/h
            ride("2019-06-01 10:00:00", "0.0", "900.0"),  # not valid
        ],
    )

    (row,) = period_metrics(frame, DAY).itertuples()

    assert (row.Index, row.trips, row.valid_trips) == ("2019-06-01", 3, 2)
    assert (row.avg_distance_m, row.avg_duration_s) == (2000.0, 600.0)
    assert row.avg_speed_kmh == pytest.approx(12.0)


def test_each_hour_counts_once_for_temperature(tmp_path):
    frame = frame_of(
        tmp_path,
        [
            ride("2019-06-01 08:05:00", temperature="10.0"),
            ride("2019-06-01 08:25:00", temperature="10.0"),
            ride("2019-06-01 08:45:00", temperature="10.0"),
            ride("2019-06-01 15:10:00", temperature="20.0"),
            ride("2019-06-01 16:10:00", temperature=""),  # no reading that hour
        ],
    )

    (row,) = period_metrics(frame, DAY).itertuples()

    assert row.avg_temperature_c == 15.0  # (10 + 20) / 2; per trip it would be 12.5
    assert row.temperature_hours == 2


def test_the_month_averages_its_trips_not_its_days(tmp_path):
    frame = frame_of(
        tmp_path,
        [
            ride("2019-06-01 08:00:00", "1000.0"),
            ride("2019-06-01 09:00:00", "1000.0"),
            ride("2019-06-01 10:00:00", "1000.0"),
            ride("2019-06-02 08:00:00", "5000.0", "1200.0"),
        ],
    )

    month = period_metrics(frame, MONTH).loc["2019-06"]

    assert month["avg_distance_m"] == 2000.0  # the days' averages would give 3000
    assert days_per_month(frame).to_dict() == {"2019-06": 2}


def test_a_day_without_valid_trips_has_no_averages(tmp_path):
    frame = frame_of(tmp_path, [ride("2019-06-01 08:00:00", "0.0")])

    (row,) = period_metrics(frame, DAY).itertuples()

    assert (row.trips, row.valid_trips) == (1, 0)
    assert math.isnan(row.avg_speed_kmh)


# --- items.py


def test_raw_items_hold_each_station_day_as_compressed_csv(tmp_path):
    rows = [
        ride("2019-06-01 09:00:00", station="B"),
        ride("2019-06-01 08:00:00", station="A"),
        ride("2019-06-01 10:00:00", station="A"),
        ride("2019-06-02 08:00:00", station="A"),
    ]
    trips = parse_trips(month_text(tmp_path, rows))

    items = list(raw_trip_items(trips))

    keys = [(i["trip_date"], i["station_part"], i["trip_count"]) for i in items]
    assert keys == [("2019-06-01", "A#0", 2), ("2019-06-01", "B#0", 1), ("2019-06-02", "A#0", 1)]
    text = gzip.decompress(items[0]["trips_csv_gz"]).decode()
    assert text.splitlines() == [trips.header, trips.lines[1], trips.lines[2]]  # source order
    assert pd.read_csv(io.StringIO(text))["departure_name"].tolist() == ["A", "A"]


def test_a_busy_station_day_is_cut_into_parts(tmp_path):
    rows = [ride(f"2019-06-01 08:00:{second:02d}") for second in range(5)]

    items = list(raw_trip_items(parse_trips(month_text(tmp_path, rows)), max_trips=2))

    assert [(i["station_part"], i["part"], i["trip_count"]) for i in items] == [
        ("A#0", 0, 2),
        ("A#1", 1, 2),
        ("A#2", 2, 1),
    ]


def test_the_same_trips_always_give_the_same_bytes(tmp_path):
    trips = parse_trips(month_text(tmp_path, [ride("2019-06-01 08:00:00")]))

    assert list(raw_trip_items(trips)) == list(raw_trip_items(trips))


def test_metrics_items_use_decimals_and_leave_out_missing_averages(tmp_path):
    frame = frame_of(
        tmp_path,
        [ride("2019-06-01 08:00:00", "1000.0", "300.0"), ride("2019-06-02 08:00:00", "0.0")],
    )

    first, second = daily_items(period_metrics(frame, DAY))
    (month,) = monthly_items(period_metrics(frame, MONTH), days_per_month(frame))

    assert first == {
        "trip_month": "2019-06",
        "trip_date": "2019-06-01",
        "trips": 1,
        "valid_trips": 1,
        "temperature_hours": 1,
        "avg_distance_m": Decimal("1000.0"),
        "avg_duration_s": Decimal("300.0"),
        "avg_speed_kmh": Decimal("12.0"),
        "avg_temperature_c": Decimal("15.0"),
    }
    assert "avg_speed_kmh" not in second
    assert (month["trip_year"], month["trip_month"], month["days"], month["trips"]) == (
        "2019",
        "2019-06",
        2,
        2,
    )


def test_station_items_come_from_the_spark_file():
    counts = 'station_name,departures,returns\n"Kamppi, M",12,10\nA.I. Virtasen aukio,89,90\n'

    assert station_items(counts, "2019-06") == [
        {"trip_month": "2019-06", "station_name": "Kamppi, M", "departures": 12, "returns": 10},
        {
            "trip_month": "2019-06",
            "station_name": "A.I. Virtasen aukio",
            "departures": 89,
            "returns": 90,
        },
    ]


def test_a_station_file_with_other_columns_is_refused():
    with pytest.raises(ValueError, match="unexpected header"):
        station_items("name,count\nA,1\n", "2019-06")
