from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
    gdp = df1.unionByName(df2)
    by_country = W.partitionBy("Country").orderBy("Year")
    previous = F.col("previous")
    growth = (F.col("GDP") - previous) / previous * 100
    return (
        gdp.withColumn("previous", F.lag("GDP").over(by_country))
        .select("Country", "Year", F.round(growth, 2).alias("GDP_growth_rate"))
        .orderBy("Country", "Year")
    )
