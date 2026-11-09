"""Departures and returns per station for one monthly file, counted with Spark"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf

from bikes.dataset import DEPARTURE_NAME, RETURN_NAME

STATION = "station_name"
CSV_OPTIONS = {"header": True, "quote": '"', "escape": '"', "encoding": "UTF-8"}


def read_trips(spark: SparkSession, path: Path) -> DataFrame:
    """A monthly trips file as strings"""
    return spark.read.options(**CSV_OPTIONS).csv(str(path))


def station_counts(trips: DataFrame) -> DataFrame:
    """station_name, departures, returns"""
    departures = trips.groupBy(sf.col(DEPARTURE_NAME).alias(STATION)).agg(
        sf.count(sf.lit(1)).alias("departures")
    )
    returns = trips.groupBy(sf.col(RETURN_NAME).alias(STATION)).agg(
        sf.count(sf.lit(1)).alias("returns")
    )
    return (
        departures.join(returns, on=STATION, how="full_outer")
        .where(sf.col(STATION).isNotNull())
        .na.fill(0, subset=["departures", "returns"])
        .orderBy(STATION)
    )


def write_csv(frame: DataFrame, path: Path) -> None:
    """Write frame as the single CSV file path"""
    path.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".spark-", dir=path.parent))
    try:
        frame.coalesce(1).write.options(**CSV_OPTIONS).mode("overwrite").csv(str(work / "out"))
        (part,) = (work / "out").glob("part-*.csv")
        part.replace(path)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def run(spark: SparkSession, trips: Path, output: Path) -> int:
    """Count one monthly file's stations into output"""
    counts = station_counts(read_trips(spark, trips)).cache()
    write_csv(counts, output)
    return counts.count()


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="station_counts",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("trips", type=Path, help="a monthly trips file")
    parser.add_argument("output", type=Path, help="where to write the counts")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    # spark-submit's --master decides where this runs; plain python runs it locally
    spark = SparkSession.builder.appName(f"station_counts {args.trips.name}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        stations = run(spark, args.trips, args.output)
        print(f"{args.output}: {stations:,} stations")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
