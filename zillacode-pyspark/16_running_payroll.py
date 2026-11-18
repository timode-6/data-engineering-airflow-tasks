from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees, payroll):
    hours = F.col("hours_worked")
    rate = F.col("hourly_rate")
    pay = F.when(hours <= 40, hours * rate).otherwise(
        40 * rate + (hours - 40) * rate * 1.5
    )
    return employees.join(payroll, "employee_id").select(
        "employee_id", "name", "position", pay.alias("pay")
    )
