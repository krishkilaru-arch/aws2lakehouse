# Databricks notebook source
# MAGIC %md
# MAGIC # partner_data_sync - Bronze Layer
# MAGIC **Domain:** finance | **Owner:** finance-ops@acmecapital.com
# MAGIC **Target:** `production.finance_bronze.partner_data_sync` | **Mode:** batch
# MAGIC **Classification:** internal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "production")
SCHEMA = os.getenv("SCHEMA", "finance_bronze")
TABLE = os.getenv("TABLE", "partner_data_sync")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# Auto Loader (json)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/production/finance_bronze/_schemas/partner_data_sync")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/Volumes/production/raw/partner_data/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (partner_data_sync_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_partner_data_sync_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline partner_data_sync complete -> {TARGET_TABLE}")
