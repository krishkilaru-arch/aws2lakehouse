# Databricks notebook source
# MAGIC %md
# MAGIC # Stream Events — Bronze (Kafka → Delta)
# MAGIC **Replaces:** Long-running EMR streaming cluster (12 executors, 24/7)
# MAGIC **Now:** Databricks streaming job with trigger(availableNow=True) — 90% cheaper

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import os

CATALOG = os.getenv("CATALOG", "production")

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("properties", MapType(StringType(), StringType())),
    StructField("page_url", StringType()),
    StructField("device", StringType()),
])

# COMMAND ----------

# Read from Kafka (same topics as EMR version)
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get("shopmax-kafka", "brokers"))
    .option("subscribe", "clickstream-events,purchase-events,cart-events")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "1000000")
    .load()
)

parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str", "topic", "partition", "offset", "timestamp as kafka_ts")
    .withColumn("event", F.from_json("json_str", event_schema))
    .select("topic", "partition", "offset", "kafka_ts", "event.*")
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Write to Delta (partitioned by event_type + date)
query = (
    parsed.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/Volumes/{CATALOG}/events_bronze/_checkpoints/kafka_events")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.events_bronze.raw_events")
)
query.awaitTermination()
print("✅ Event streaming complete")

