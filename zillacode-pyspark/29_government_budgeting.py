from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(budget_df, spending_df):
    budget = budget_df.groupBy("Department").agg(
        F.variance("Budget").alias("budget_var")
    )
    spending = spending_df.groupBy("Department").agg(
        F.variance("Spending").alias("spending_var")
    )
    # cast("int") cuts the fraction off (3433.33 becomes 3433), as the tests expect
    return budget.join(spending, "Department").select(
        "Department",
        F.col("budget_var").cast("int").alias("Budget_Variance"),
        F.col("spending_var").cast("int").alias("Spending_Variance"),
    )
