from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(products_df, manufacturing_processes_df):
    products = products_df.dropDuplicates()
    processes = manufacturing_processes_df.dropDuplicates()
    return products.join(processes, "ProductID")
