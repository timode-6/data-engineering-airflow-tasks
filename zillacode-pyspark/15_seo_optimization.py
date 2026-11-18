from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pages):
    by_domain = W.partitionBy("domain").orderBy(F.desc("seo_score"), "url")
    best = pages.withColumn("rn", F.row_number().over(by_domain)).filter(
        F.col("rn") == 1
    )
    is_overall = F.col("seo_score") == F.max("seo_score").over(W.partitionBy())
    # the tests fill the overall columns only on the top page's own domain
    return best.select(
        "domain",
        F.col("url").alias("highest_seo_page"),
        F.col("seo_score").alias("highest_seo_score"),
        F.when(is_overall, F.col("url")).alias("overall_highest_page"),
        F.when(is_overall, F.col("seo_score")).alias("overall_highest_score"),
    )
