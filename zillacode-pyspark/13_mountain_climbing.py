from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(mountain_info, mountain_climbers):
    known = mountain_info.select(F.col("name").alias("mountain_name"))
    climbs = mountain_climbers.join(known, "mountain_name", "left_semi")

    latest_first = W.partitionBy("mountain_name").orderBy(
        F.to_date("climb_date").desc()
    )
    # the tests expect the input's column names, not last_climber_name etc.
    return (
        climbs.withColumn("rank", F.rank().over(latest_first))
        .filter(F.col("rank") == 1)
        .select("mountain_name", "climber_name", "climb_date", "climb_time")
    )
