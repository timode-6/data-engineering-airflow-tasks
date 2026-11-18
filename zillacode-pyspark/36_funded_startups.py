from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(venture_capitalist_df, funded_startups_df):
    avg_funding = funded_startups_df.groupBy("vc_id").agg(
        F.avg("funding").alias("avg_funding")
    )
    # the expected output has funding_limit too, so all columns stay
    return venture_capitalist_df.join(avg_funding, "vc_id").filter(
        F.col("avg_funding") > F.col("funding_limit")
    )
