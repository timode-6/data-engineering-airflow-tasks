from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(background_checks):
    def count_items(column):
        value = F.col(column)
        return F.when(value.isNull() | (value == ""), 0).otherwise(
            F.size(F.split(value, ","))
        )

    return background_checks.select(
        "check_id",
        "full_name",
        "dob",
        count_items("criminal_record").alias("crime_count"),
        count_items("employment_history").alias("jobs_count"),
        count_items("education_history").alias("degrees_count"),
        count_items("address").alias("places_lived_count"),
    )
