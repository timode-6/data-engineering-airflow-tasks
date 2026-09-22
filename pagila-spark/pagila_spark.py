from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

JDBC_DRIVER_PACKAGE = "org.postgresql:postgresql:42.7.3"

JDBC_URL = os.environ.get("PAGILA_JDBC_URL", "jdbc:postgresql://127.0.0.1:5433/pagila")
JDBC_USER = os.environ.get("PAGILA_USER", "postgres")

JDBC_PASSWORD = os.environ.get("PAGILA_PASSWORD")
if not JDBC_PASSWORD:
    raise SystemExit("PAGILA_PASSWORD is not set")

def create_session() -> SparkSession:
    return (
        SparkSession.builder.appName("pagila_spark")
        .config("spark.jars.packages", JDBC_DRIVER_PACKAGE)
        .getOrCreate()
    )


def read_table(spark: SparkSession, table: str, *columns: str) -> DataFrame:
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
        "city": read_table(spark, "city", "city_id", "city"),
    }


def films_per_category(t: dict[str, DataFrame]) -> DataFrame:
    return (
        t["category"]
        .join(t["film_category"], "category_id", "left")
        .groupBy("category_id", "category")
        .agg(F.count("film_id").alias("films"))
        .orderBy(F.desc("films"), "category")
        .select("category", "films")
    )


def top_rented_actors(t: dict[str, DataFrame], n: int = 10) -> DataFrame:
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
    return (
        t["category"]
        .join(t["film_category"], "category_id")
        .join(t["inventory"], "film_id")
        .join(t["rental"], "inventory_id")
        .join(t["payment"], "rental_id")
        .groupBy("category_id", "category")
        .agg(F.sum("amount").alias("revenue"))
        .orderBy(F.desc("revenue"))
        .limit(1)
        .select("category", "revenue")
    )


def films_not_in_inventory(t: dict[str, DataFrame]) -> DataFrame:
    return t["film"].join(t["inventory"], "film_id", "left_anti").orderBy("title")


def top_children_actors(t: dict[str, DataFrame], n: int = 3) -> DataFrame:
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
    return (
        t["city"]
        .join(t["address"], "city_id")
        .join(t["customer"], "address_id")
        .groupBy("city_id", "city")
        .agg(
            F.count(F.when(F.col("active") == 1, True)).alias("active_customers"),
            F.count(F.when(F.col("active") == 0, True)).alias("inactive_customers"),
        )
        .orderBy(F.desc("inactive_customers"), "city")
        .select("city", "active_customers", "inactive_customers")
    )


def top_category_by_rental_hours(t: dict[str, DataFrame]) -> DataFrame:
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
        per_group.withColumn("rn", F.row_number().over(by_hours))
        .filter(F.col("rn") == 1)
        .select(
            "city_group",
            "category",
            F.round("total_hours", 2).alias("total_rental_hours"),
        )
        .orderBy("city_group")
    )


def show_all(title: str, df: DataFrame) -> None:
    print(f"\n{title}")
    df.show(df.count(), truncate=False)


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
