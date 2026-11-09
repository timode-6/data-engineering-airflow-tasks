"""The Spark job, on a real local Spark, needs Java 17 or 21"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from bikes.station_counts import read_trips, run, station_counts  # noqa: E402
from helpers import read_rows, trip, write_source  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("bikes tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception as error:  # no Java, or one this Spark cannot run on
        pytest.skip(f"Spark did not start; it needs Java 17 or 21 in JAVA_HOME ({error})")
    # Newer Javas start Spark but fail its first file read
    java = session.sparkContext._jvm.java.lang.System.getProperty("java.specification.version")
    if int(java) > 21:
        session.stop()
        pytest.skip(f"Spark runs on Java {java} here; it needs Java 17 or 21 in JAVA_HOME")
    yield session
    session.stop()


def rides(*pairs: tuple[str, str]) -> list[dict[str, str]]:
    """One trip per (from, to) pair of station names"""
    return [
        trip(f"2019-06-01 08:00:{n:02d}", departure_name=start, return_name=end)
        for n, (start, end) in enumerate(pairs)
    ]


def test_each_station_gets_its_departures_and_returns(spark, tmp_path):
    source = write_source(tmp_path, rides(("A", "B"), ("A", "C"), ("B", "A"), ("D", "A")))

    rows = station_counts(read_trips(spark, source)).collect()

    assert [(r.station_name, r.departures, r.returns) for r in rows] == [
        ("A", 2, 2),
        ("B", 1, 1),
        ("C", 0, 1),  # only ever returned to
        ("D", 1, 0),  # only ever left from
    ]


def test_a_trip_without_a_station_name_is_not_a_station(spark, tmp_path):
    source = write_source(tmp_path, rides(("A", ""), ("", "A")))

    rows = station_counts(read_trips(spark, source)).collect()

    assert [(r.station_name, r.departures, r.returns) for r in rows] == [("A", 1, 1)]


def test_the_job_writes_one_csv_that_keeps_awkward_names(spark, tmp_path):
    awkward = 'Kamppi, "Kauppakeskus"'
    trips = rides((awkward, "Länsisatamankatu"), ("Länsisatamankatu", awkward))
    source = write_source(tmp_path, trips)
    output = tmp_path / "metrics" / "station_counts_2019-06.csv"

    assert run(spark, source, output) == 2

    assert read_rows(output) == [
        {"station_name": awkward, "departures": "1", "returns": "1"},
        {"station_name": "Länsisatamankatu", "departures": "1", "returns": "1"},
    ]
    assert [path.name for path in output.parent.iterdir()] == [output.name]  # no Spark leftovers
