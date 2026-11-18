from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(projects, employees, equipment):
    staff = employees.groupBy("project_id").agg(
        F.count("employee_id").alias("total_employees"),
        F.countDistinct("role").alias("unique_roles"),
    )
    costs = equipment.groupBy("project_id").agg(
        F.sum("cost").alias("total_equipment_cost")
    )
    duration = F.datediff(F.to_date("end_date"), F.to_date("start_date"))
    return (
        projects.join(staff, "project_id", "left")
        .join(costs, "project_id", "left")
        .select(
            "project_id",
            "project_name",
            "start_date",
            "end_date",
            duration.alias("duration_days"),
            "total_employees",
            "unique_roles",
            "total_equipment_cost",
        )
    )
