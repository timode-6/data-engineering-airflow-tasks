from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df):
    digits = F.regexp_extract("Description", r"\[(\d{1,3})%\s*off\]", 1)
    discount = F.when(digits == "", 0.0).otherwise(digits.cast("int") / 100)
    return df.withColumn("Discount", discount)
