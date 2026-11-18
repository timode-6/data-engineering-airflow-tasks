from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(companies, investments):
    totals = (
        investments.join(F.broadcast(companies), "company_id")
        .groupBy("industry")
        .agg(F.sum("amount").alias("total_investment"))
    )
    return totals.orderBy(F.desc("total_investment"))
