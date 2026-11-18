from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
    name = F.split("full_name", " ", 2)
    customers = customers.select(
        "customer_id",
        name.getItem(0).alias("first_name"),
        name.getItem(1).alias("last_name"),
        "location",
    )
    info = F.split("product_info", ",")
    products = products.select(
        "product_id",
        F.trim(info.getItem(0)).alias("product_type"),
        F.trim(info.getItem(1)).alias("product_color"),
    )
    return orders.join(customers, "customer_id", "left").join(
        products, "product_id", "left"
    )
