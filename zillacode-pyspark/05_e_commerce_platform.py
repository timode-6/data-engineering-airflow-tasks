from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(products_df, orders_df):
    orders = products_df.join(orders_df, "product_id")
    return orders.groupBy("category").agg(
        F.avg("price").alias("avg_price"),
        F.count("order_id").alias("total_orders_count"),
    )
