from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(transactions):
    by_customer = W.partitionBy("customer_id").orderBy(F.to_date("date"))
    previous = F.lag("product_id").over(by_customer)
    return transactions.withColumn("previous_product", previous).withColumn(
        "date_and_product",
        F.concat_ws(
            " ", "date", F.coalesce(F.col("previous_product"), F.lit("None"))
        ),
    )
