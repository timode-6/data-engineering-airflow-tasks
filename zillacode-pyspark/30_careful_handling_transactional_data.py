from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_transactions, df_clients):
    def first_copies(df, key):
        numbered = df.withColumn("_order", F.monotonically_increasing_id())
        first = W.partitionBy(key).orderBy("_order")
        return (
            numbered.withColumn("_n", F.row_number().over(first))
            .filter(F.col("_n") == 1)
            .drop("_order", "_n")
        )

    transactions = first_copies(df_transactions, "TransactionID").filter(
        (F.col("TransactionID") > 0) & F.col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$")
    )
    clients = first_copies(df_clients, "ClientID").filter(F.col("ClientID") > 0)
    # the inner join drops transactions whose client is unknown or invalid
    return transactions.join(clients, "ClientID")
