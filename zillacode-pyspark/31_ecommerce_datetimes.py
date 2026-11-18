from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_orders, df_products):
    order_date = F.to_date("order_date", "M/d/yyyy")
    # dayofweek: 1 is Sunday, 7 is Saturday
    is_weekend = F.dayofweek(order_date).isin(1, 7)
    return df_orders.join(df_products, "product_id").select(
        "user_id",
        "product_name",
        "category",
        "order_date",
        is_weekend.alias("is_weekend"),
    )
