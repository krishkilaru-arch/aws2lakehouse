# Databricks notebook source
# MAGIC %md
# MAGIC # daily_risk_aggregation - Bronze Layer
# MAGIC **Domain:** risk | **Owner:** risk-engineering@company.com
# MAGIC **Target:** `acme_prod.risk_bronze.daily_risk_aggregation` | **Mode:** batch
# MAGIC **Classification:** internal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "risk_bronze")
TABLE = os.getenv("TABLE", "daily_risk_aggregation")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

df = spark.table("")

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (daily_risk_aggregation_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_daily_risk_aggregation_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline daily_risk_aggregation complete -> {TARGET_TABLE}")
