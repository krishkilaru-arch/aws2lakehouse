# Databricks notebook source
# MAGIC %md
# MAGIC # customer_360 - Bronze Layer
# MAGIC **Domain:** customer | **Owner:** customer-platform@company.com
# MAGIC **Target:** `acme_prod.customer_bronze.customer_360` | **Mode:** batch
# MAGIC **Classification:** confidential

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "customer_bronze")
TABLE = os.getenv("TABLE", "customer_360")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# Read from Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get("kafka", "bootstrap_servers"))
    .option("subscribe", "events")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "100000")
    .load()
)
df = (
    raw_df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_kafka_offset", F.col("offset"))
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (customer_360_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_customer_360_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline customer_360 complete -> {TARGET_TABLE}")
