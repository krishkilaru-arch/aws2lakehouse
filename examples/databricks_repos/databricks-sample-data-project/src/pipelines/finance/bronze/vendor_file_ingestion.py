# Databricks notebook source
# MAGIC %md
# MAGIC # vendor_file_ingestion - Bronze Layer
# MAGIC **Domain:** finance | **Owner:** data-team@company.com
# MAGIC **Target:** `acme_prod.finance_bronze.vendor_file_ingestion` | **Mode:** batch
# MAGIC **Classification:** internal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "finance_bronze")
TABLE = os.getenv("TABLE", "vendor_file_ingestion")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# Auto Loader (json)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/acme_prod/finance_bronze/_schemas/vendor_file_ingestion")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/Volumes/acme_prod/raw/vendor_file_ingestion/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (vendor_file_ingestion_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_vendor_file_ingestion_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline vendor_file_ingestion complete -> {TARGET_TABLE}")
