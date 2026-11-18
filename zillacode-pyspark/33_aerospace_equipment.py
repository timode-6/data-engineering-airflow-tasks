from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(aerospace_df, company_df):
    companies = company_df.select(
        F.col("id").alias("company_id"),
        F.col("name").alias("company_name"),
        "country",
    )
    is_active = F.col("status") == "active"
    status_label = (
        F.when(is_active & (F.col("country") == "USA"), "Domestic Active")
        .when(is_active & (F.col("country") != "USA"), "Foreign Active")
        .otherwise("Inactive")
    )
    return aerospace_df.join(companies, "company_id", "left").select(
        "id",
        F.col("name").alias("equipment_name"),
        F.col("type").alias("equipment_type"),
        F.col("status").alias("equipment_status"),
        "company_name",
        "country",
        status_label.alias("status_label"),
    )
