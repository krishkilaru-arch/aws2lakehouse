# Databricks notebook source
# MAGIC %md
# MAGIC # payment_processing - Bronze Layer
# MAGIC **Domain:** lending | **Owner:** lending-team@acmecapital.com
# MAGIC **Target:** `production.lending_bronze.payment_processing` | **Mode:** streaming
# MAGIC **Classification:** confidential

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "production")
SCHEMA = os.getenv("SCHEMA", "lending_bronze")
TABLE = os.getenv("TABLE", "payment_processing")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# JDBC: public.payments
df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("lending-postgres", "url"))
    .option("user", dbutils.secrets.get("lending-postgres", "username"))
    .option("password", dbutils.secrets.get("lending-postgres", "password"))
    .option("dbtable", "public.payments")
    .option("partitionColumn", "payment_id")
    .option("numPartitions", "20")
    .option("fetchsize", "10000")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (payment_processing_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_payment_processing_id"

# COMMAND ----------

# Write: Streaming append
query = (
    df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/production/lending_bronze/_checkpoints/payment_processing")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

# COMMAND ----------
print(f"Pipeline payment_processing complete -> {TARGET_TABLE}")
