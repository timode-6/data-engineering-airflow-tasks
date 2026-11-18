from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_models, df_usage):
    by_type = W.partitionBy("Model_Type")
    models = df_models.withColumn(
        "Average_Accuracy", F.avg("Accuracy").over(by_type)
    )
    total_uses = df_usage.groupBy("Model_ID").agg(F.sum("Uses").alias("Total_Uses"))
    return models.join(total_uses, "Model_ID")
