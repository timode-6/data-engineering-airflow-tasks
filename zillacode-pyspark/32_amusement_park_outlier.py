from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(rides, visitors):
    ride_avg = visitors.groupBy("ride_id").agg(
        F.avg("rating").alias("average_rating")
    )
    overall = F.avg("average_rating").over(W.partitionBy())
    # "significantly" isn't defined in the task; the tests use more than 50%
    # above or below the overall average
    rating = F.col("average_rating")
    is_anomalous = (rating < 0.5 * overall) | (rating > 1.5 * overall)
    return rides.join(ride_avg.withColumn("is_anomalous", is_anomalous), "ride_id")
