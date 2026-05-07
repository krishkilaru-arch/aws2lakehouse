# Databricks notebook source
# MAGIC %md
# MAGIC # ecommerce_daily_etl - Bronze Layer
# MAGIC **Domain:** analytics | **Owner:** data-engineering@company.com
# MAGIC **Target:** `ecommerce_prod.analytics_bronze.ecommerce_daily_etl` | **Mode:** batch
# MAGIC **Classification:** internal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "ecommerce_prod")
SCHEMA = os.getenv("SCHEMA", "analytics_bronze")
TABLE = os.getenv("TABLE", "ecommerce_daily_etl")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

df = spark.table("")

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (ecommerce_daily_etl_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_ecommerce_daily_etl_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline ecommerce_daily_etl complete -> {TARGET_TABLE}")
