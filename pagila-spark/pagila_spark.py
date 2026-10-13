"""Pagila analytics in PySpark — seven questions, DataFrame API only, no SQL.

Tables are read from PostgreSQL over JDBC. Tested against Pagila v3.1.0 on
PostgreSQL 16, with Spark 3.1.3 on Java 8 and Python 3.9.

Configuration comes from the environment. The script does NOT read .env itself —
docker-compose does that for the container, but a Python process does not, so the
variables have to be exported into the shell first:

    bash/zsh:   set -a; source .env; set +a
    fish:       for l in (grep -v '^#' .env | string match -er .); set -gx (string split -m1 = $l); end

Then:

    python pagila_spark.py

    PAGILA_JDBC_URL    default jdbc:postgresql://127.0.0.1:5433/pagila
    PAGILA_USER        default postgres
    PAGILA_PASSWORD    no default — the script exits if it is unset

The PostgreSQL JDBC driver is fetched from Maven Central on first run via
spark.jars.packages. That only works when this script creates the session itself, as
above. Under spark-submit the driver JVM is already running, so pass it explicitly:

    spark-submit --packages org.postgresql:postgresql:42.7.3 pagila_spark.py
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MAX_DISPLAY_ROWS = 50

JDBC_DRIVER_PACKAGE = "org.postgresql:postgresql:42.7.3"  # the Java 8 build

JDBC_URL = os.environ.get("PAGILA_JDBC_URL", "jdbc:postgresql://127.0.0.1:5433/pagila")
JDBC_USER = os.environ.get("PAGILA_USER", "postgres")

JDBC_PASSWORD = os.environ.get("PAGILA_PASSWORD")
if not JDBC_PASSWORD:
    raise SystemExit(
        "PAGILA_PASSWORD is not set. Export it first - see the Setup section of "
        "README.md. Sourcing .env is not enough for a Python process."
    )


# loading


def create_session() -> SparkSession:
    return (
        SparkSession.builder.appName("pagila_spark")
        .config("spark.jars.packages", JDBC_DRIVER_PACKAGE)
        .getOrCreate()
    )


def read_table(spark: SparkSession, table: str, *columns: str) -> DataFrame:
    """Read one table over JDBC, projecting only the columns the queries use.

    The projection is pushed into the SELECT the driver sends, so unused columns
    never cross the wire. cache() matters because most tables are read by several
    queries; without it each one re-queries PostgreSQL.
    """
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "1000")
        .load()
        .select(*columns)
        .cache()
    )


def load_tables(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "category": read_table(
            spark, "category", "category_id", "name"
        ).withColumnRenamed("name", "category"),
        "film": read_table(spark, "film", "film_id", "title"),
        "film_category": read_table(spark, "film_category", "film_id", "category_id"),
        "actor": read_table(spark, "actor", "actor_id", "first_name", "last_name"),
        "film_actor": read_table(spark, "film_actor", "actor_id", "film_id"),
        "inventory": read_table(spark, "inventory", "inventory_id", "film_id"),
        "rental": read_table(
            spark,
            "rental",
            "rental_id",
            "inventory_id",
            "customer_id",
            "rental_date",
            "return_date",
        ),
        "payment": read_table(spark, "payment", "rental_id", "amount"),
        "customer": read_table(
            spark, "customer", "customer_id", "address_id", "active"
        ),
        "address": read_table(spark, "address", "address_id", "city_id"),
        "city": read_table(spark, "city", "city_id", "city", "country_id"),
        "country": read_table(spark, "country", "country_id", "country"),
    }


# queries


def films_per_category(t: dict[str, DataFrame]) -> DataFrame:
    """1. Number of films in each category, descending"""
    return (
        t["category"]
        .join(t["film_category"], "category_id", "left")
        .groupBy("category_id", "category")
        .agg(F.count("film_id").alias("films"))
        .orderBy(F.desc("films"), "category")
        .select("category", "films")
    )


def top_rented_actors(t: dict[str, DataFrame], n: int = 10) -> DataFrame:
    """2. The ten actors whose films were rented most often"""
    return (
        t["actor"]
        .join(t["film_actor"], "actor_id")
        .join(t["inventory"], "film_id")
        .join(t["rental"], "inventory_id")
        .groupBy("actor_id", "first_name", "last_name")
        .agg(F.count("rental_id").alias("rentals"))
        .orderBy(F.desc("rentals"), "actor_id")
        .limit(n)
    )


def top_revenue_category(t: dict[str, DataFrame]) -> DataFrame:
    """3. The category that earned the most money"""
    by_revenue = Window.orderBy(F.desc("revenue"))
    return (
        t["category"]
        .join(t["film_category"], "category_id")
        .join(t["inventory"], "film_id")
        .join(t["rental"], "inventory_id")
        .join(t["payment"], "rental_id")
        .groupBy("category_id", "category")
        .agg(F.sum("amount").alias("revenue"))
        .withColumn("rank", F.rank().over(by_revenue))
        .filter(F.col("rank") == 1)
        .select("category", "revenue")
    )


def films_not_in_inventory(t: dict[str, DataFrame]) -> DataFrame:
    """4. Titles of films absent from inventory"""
    return (
        t["film"]
        .join(t["inventory"], "film_id", "left_anti")
        .orderBy("title")
        .select("title")
    )


def top_children_actors(t: dict[str, DataFrame], n: int = 3) -> DataFrame:
    """5. The three actors appearing most often in "Children" films"""
    children = t["category"].filter(F.col("category") == "Children")
    film_counts = (
        t["actor"]
        .join(t["film_actor"], "actor_id")
        .join(t["film_category"], "film_id")
        .join(children, "category_id")
        .groupBy("actor_id", "first_name", "last_name")
        .agg(F.count("film_id").alias("films"))
    )
    by_films = Window.orderBy(F.desc("films"))
    return (
        film_counts.withColumn("rank", F.rank().over(by_films))
        .filter(F.col("rank") <= n)
        .drop("rank")
        .orderBy(F.desc("films"), "last_name", "first_name")
    )


def customers_by_city(t: dict[str, DataFrame]) -> DataFrame:
    """6. Active and inactive customers per city"""
    return (
        t["city"]
        .join(t["country"], "country_id")
        .join(t["address"], "city_id")
        .join(t["customer"], "address_id")
        .groupBy("city_id", "city", "country")
        .agg(
            F.count(F.when(F.col("active") == 1, True)).alias("active_customers"),
            F.count(F.when(F.col("active") == 0, True)).alias("inactive_customers"),
        )
        .orderBy(F.desc("inactive_customers"), "city", "country")
        .select("city", "country", "active_customers", "inactive_customers")
    )


def top_category_by_rental_hours(t: dict[str, DataFrame]) -> DataFrame:
    """7. Category with the most rental hours, for cities starting with "a" and,
    separately, cities containing "-" """
    rentals = (
        t["rental"]
        .filter(F.col("return_date").isNotNull())
        .withColumn(
            "hours",
            (F.col("return_date").cast("long") - F.col("rental_date").cast("long"))
            / 3600.0,
        )
        .join(t["inventory"], "inventory_id")
        .join(t["film_category"], "film_id")
        .join(t["category"], "category_id")
        .join(t["customer"], "customer_id")
        .join(t["address"], "address_id")
        .join(t["city"], "city_id")
    )

    def hours_by_category(city_filter, label: str) -> DataFrame:
        return (
            rentals.filter(city_filter)
            .groupBy("category")
            .agg(F.sum("hours").alias("total_hours"))
            .withColumn("city_group", F.lit(label))
        )

    per_group = hours_by_category(
        F.lower(F.col("city")).startswith("a"), 'cities starting with "a"'
    ).unionByName(
        hours_by_category(F.col("city").contains("-"), 'cities containing "-"')
    )

    by_hours = Window.partitionBy("city_group").orderBy(F.desc("total_hours"))
    return (
        per_group.withColumn("rank", F.rank().over(by_hours))
        .filter(F.col("rank") == 1)
        .select(
            "city_group",
            "category",
            F.round("total_hours", 2).alias("total_rental_hours"),
        )
        .orderBy("city_group")
    )


# output


def show_all(title: str, df: DataFrame) -> None:
    """Print the total row count, then at most MAX_DISPLAY_ROWS rows"""
    df = df.cache()
    try:
        rows = df.count()
        print(f"\n{title} — {rows} rows, showing up to {MAX_DISPLAY_ROWS}")
        df.show(MAX_DISPLAY_ROWS, truncate=False)
    finally:
        df.unpersist()


def main() -> None:
    spark = create_session()
    spark.sparkContext.setLogLevel("WARN")
    tables = load_tables(spark)

    show_all("1. Films per category", films_per_category(tables))
    show_all("2. Ten most-rented actors", top_rented_actors(tables))
    show_all("3. Highest-earning category", top_revenue_category(tables))
    show_all("4. Films not in inventory", films_not_in_inventory(tables))
    show_all('5. Top actors in "Children", including ties', top_children_actors(tables))
    show_all("6. Active and inactive customers per city", customers_by_city(tables))
    show_all(
        "7. Most rental hours by category, per city group",
        top_category_by_rental_hours(tables),
    )

    spark.stop()


if __name__ == "__main__":
    main()
