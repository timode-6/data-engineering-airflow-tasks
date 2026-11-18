from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
    phone = F.col("phone").cast("string")
    return input_df.select(
        "user_id",
        F.regexp_extract("email", r"@(.+)$", 1).alias("email_domain"),
        F.regexp_replace(phone, r"^\d{6}", "******").alias("anon_phone"),
    )
