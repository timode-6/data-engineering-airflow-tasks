from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(calls_df, customers_df):
    known_calls = calls_df.join(F.broadcast(customers_df), "cust_id", "left_semi")
    return known_calls.groupBy("date").agg(
        F.countDistinct("cust_id").alias("num_customers"),
        F.sum("duration").alias("total_duration"),
    )
