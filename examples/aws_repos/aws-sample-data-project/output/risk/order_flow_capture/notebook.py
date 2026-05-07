# Databricks notebook source
# MAGIC %md
# MAGIC # order_flow_capture - Bronze Layer
# MAGIC **Domain:** risk | **Owner:** risk-engineering@acmecapital.com
# MAGIC **Target:** `production.risk_bronze.order_flow_capture` | **Mode:** streaming
# MAGIC **Classification:** internal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "production")
SCHEMA = os.getenv("SCHEMA", "risk_bronze")
TABLE = os.getenv("TABLE", "order_flow_capture")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# Read from Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get("kafka-risk-prod", "bootstrap_servers"))
    .option("subscribe", "order-flow")
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
assert df.filter("NOT (order_flow_capture_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_order_flow_capture_id"

# COMMAND ----------

# Write: Streaming append
query = (
    df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/production/risk_bronze/_checkpoints/order_flow_capture")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

# COMMAND ----------
print(f"Pipeline order_flow_capture complete -> {TARGET_TABLE}")
