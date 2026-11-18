from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(research_papers, authors):
    by_paper = W.partitionBy("paper_id").orderBy("author_id")
    return authors.withColumn("row_number", F.row_number().over(by_paper))
