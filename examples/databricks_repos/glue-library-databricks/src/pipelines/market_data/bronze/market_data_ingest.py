# Databricks notebook source
# MAGIC %md
# MAGIC # Market Data Ingest — Bronze Layer
# MAGIC **Domain:** market-data | **Owner:** risk-engineering@acmecapital.com
# MAGIC **Library:** acme_datalib v3.0.0 (installed from Volumes)

# COMMAND ----------

# MAGIC %pip install /Volumes/production/libraries/acme_datalib-3.0.0-py3-none-any.whl --quiet

# COMMAND ----------

from acme_datalib.connectors.bloomberg import BloombergConnector
from acme_datalib.transformations.market_data import normalize_bloomberg_feed, detect_stale_prices
from acme_datalib.quality.validators import DataValidator
from pyspark.sql import functions as F
import os

CATALOG = os.getenv("CATALOG", "production")
TARGET_TABLE = f"{CATALOG}.market_data_bronze.bloomberg_pricing"

# COMMAND ----------

# Auto Loader: stream new Bloomberg files as they land in Volume
bloomberg = BloombergConnector(spark, {"volume_path": "/Volumes/production/raw/bloomberg"})
raw_stream = bloomberg.read_daily_pricing_stream()

# COMMAND ----------

# Transform using shared library (same code as AWS version!)
from acme_datalib.transformations.market_data import normalize_bloomberg_feed

normalized = (
    raw_stream
    .transform(normalize_bloomberg_feed)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

# Write as streaming Delta table with Auto Loader
query = (
    normalized.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/Volumes/{CATALOG}/market_data_bronze/_checkpoints/bloomberg")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()
print(f"✅ Market data ingest complete → {TARGET_TABLE}")

