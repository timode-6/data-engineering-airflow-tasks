from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
    by_equipment = W.partitionBy("equipment_id")
    maintenance = df2.select(
        "equipment_id",
        "maintenance_date",
        F.max(F.to_date("maintenance_date")).over(by_equipment).alias("latest"),
        F.dense_rank()
        .over(by_equipment.orderBy(F.desc("maintenance_cost")))
        .alias("maintenance_cost_rank"),
    )
    latest = maintenance.filter(
        F.to_date("maintenance_date") == F.col("latest")
    ).select(
        "equipment_id",
        F.col("maintenance_date").alias("latest_maintenance_date"),
        "maintenance_cost_rank",
    )
    # the inner join leaves out equipment without maintenance records
    return df1.join(latest, "equipment_id").select(
        "equipment_id",
        "equipment_name",
        "purchase_date",
        "latest_maintenance_date",
        "maintenance_cost_rank",
    )
