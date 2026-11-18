from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(products, sales, inventory):
    sales_totals = sales.groupBy("product_id").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("revenue").alias("total_revenue"),
    )
    stock_totals = inventory.groupBy("product_id").agg(
        F.sum("stock").alias("total_stock")
    )
    return (
        products.join(sales_totals, "product_id", "left")
        .join(stock_totals, "product_id", "left")
        .fillna(0, subset=["total_quantity", "total_revenue", "total_stock"])
    )
