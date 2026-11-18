from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
    return (
        orders.join(F.broadcast(customers), "customer_id")
        .join(F.broadcast(products), "product_id")
        .select(
            "order_id",
            # concat_ws skips nulls; concat would make the whole name null
            F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
            "email",
            "product_name",
            "category",
            "order_date",
        )
    )
