from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_accounts, df_activities, df_exit_surveys):
    activities = df_activities.dropDuplicates(
        ["user_id", "activity_date", "activity_type"]
    )
    combined = df_accounts.join(activities, "user_id", "full").join(
        df_exit_surveys, "user_id", "full"
    )
    return combined.orderBy("user_id", F.to_date("activity_date").desc())
