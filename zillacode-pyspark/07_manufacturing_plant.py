from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(products, sales):
    revenue = sales.groupBy("product_id").agg(F.sum("revenue").alias("revenue"))
    # dense_rank: tied products share a rank and the next one follows without
    # a gap (rank would skip a number, row_number would split the tie)
    by_category = W.partitionBy("category").orderBy(F.desc("revenue"))
    ranked = products.join(revenue, "product_id").select(
        "category",
        "product_name",
        "revenue",
        F.dense_rank().over(by_category).alias("rank"),
    )
    return ranked.filter(F.col("rank") <= 3)
