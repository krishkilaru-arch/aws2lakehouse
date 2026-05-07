# Databricks notebook source
# MAGIC %md
# MAGIC # market_data_feed - Bronze Layer
# MAGIC **Domain:** risk | **Owner:** data-team@company.com
# MAGIC **Target:** `acme_prod.risk_bronze.market_data_feed` | **Mode:** streaming
# MAGIC **Classification:** mnpi

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "risk_bronze")
TABLE = os.getenv("TABLE", "market_data_feed")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# Read from Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get("kafka", "bootstrap_servers"))
    .option("subscribe", "market-data-feed")
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
assert df.filter("NOT (market_data_feed_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_market_data_feed_id"

# COMMAND ----------

# Write: Streaming append to Delta
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/acme_prod/risk_bronze/_checkpoints/market_data_feed")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()

# COMMAND ----------
print(f"Pipeline market_data_feed complete -> {TARGET_TABLE}")
