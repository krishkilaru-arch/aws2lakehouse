# Databricks notebook source
# MAGIC %md
# MAGIC # payment_processing - Bronze Layer
# MAGIC **Domain:** lending | **Owner:** data-team@company.com
# MAGIC **Target:** `acme_prod.lending_bronze.payment_processing` | **Mode:** batch
# MAGIC **Classification:** confidential

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "lending_bronze")
TABLE = os.getenv("TABLE", "payment_processing")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# JDBC: payment_processing
df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("jdbc", "url"))
    .option("user", dbutils.secrets.get("jdbc", "username"))
    .option("password", dbutils.secrets.get("jdbc", "password"))
    .option("dbtable", "payment_processing")
    .option("partitionColumn", "id")
    .option("numPartitions", "20")
    .option("fetchsize", "10000")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (payment_processing_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_payment_processing_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline payment_processing complete -> {TARGET_TABLE}")
