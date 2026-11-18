from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(flights, airports, planes):
    origins = airports.select(
        F.col("airport_id").alias("origin_airport"),
        F.length(F.trim("airport_name")).alias("origin_airport_name_length"),
    )
    destinations = airports.select(
        F.col("airport_id").alias("destination_airport"),
        F.length(F.trim("airport_name")).alias("destination_airport_name_length"),
    )
    models = planes.select(
        F.col("plane_id").alias("flight_id"),
        F.length(F.trim("plane_model")).alias("plane_model_length"),
    )
    return (
        flights.join(F.broadcast(origins), "origin_airport", "left")
        .join(F.broadcast(destinations), "destination_airport", "left")
        .join(F.broadcast(models), "flight_id", "left")
        .select(
            "flight_id",
            "origin_airport_name_length",
            "destination_airport_name_length",
            "plane_model_length",
        )
    )
