from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(properties_df, landlords_df):
    properties = properties_df.dropDuplicates()
    landlords = landlords_df.dropDuplicates()

    by_type = properties.groupBy("landlord_id").pivot("property_type").agg(
        F.sum("rent")
    )
    types = [c for c in by_type.columns if c != "landlord_id"]
    total = sum((F.coalesce(F.col(c), F.lit(0)) for c in types), F.lit(0))

    return by_type.join(landlords, "landlord_id").select(
        "landlord_id",
        F.concat_ws(" ", "first_name", "last_name").alias("landlord_name"),
        # double, not float: float keeps about 7 digits and would round big sums
        total.cast("double").alias("total_rental_income"),
    )
