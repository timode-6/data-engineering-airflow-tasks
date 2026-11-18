from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
    by_date = W.orderBy(F.to_date("manufacturing_date"))
    return df1.join(df2, "product_id").withColumn(
        "row_number", F.row_number().over(by_date)
    )
