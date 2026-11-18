from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(user_behavior_df, subscription_df):
    watched = F.to_date(F.col("w.date"))
    start = F.to_date(F.col("s.subscriptionStart"))
    end = F.col("s.subscriptionEnd")
    active = (watched >= start) & ((end == "ongoing") | (watched <= F.to_date(end)))
    covered = user_behavior_df.alias("w").join(
        subscription_df.alias("s"),
        (F.col("w.userId") == F.col("s.userId")) & active,
        "left_semi",
    )
    return covered.groupBy("userId").agg(
        F.sum("watchDuration").alias("totalWatchTime")
    )
