from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
    return input_df.filter(
        (F.col("view_count") > 1_000_000) & (F.col("release_year") >= 2024 - 5)
    )
