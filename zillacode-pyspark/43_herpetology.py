from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(observations, species):
    by_count = W.orderBy(F.desc("count"))
    return (
        observations.join(species, "species_id")
        .withColumn("rank", F.rank().over(by_count))
        .filter(F.col("rank") <= 3)
        .drop("rank")
    )
