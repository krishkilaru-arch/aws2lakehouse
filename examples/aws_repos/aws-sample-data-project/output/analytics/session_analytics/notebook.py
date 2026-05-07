# Databricks notebook source
# MAGIC %md
# MAGIC # session_analytics - Bronze Layer
# MAGIC **Domain:** analytics | **Owner:** analytics-team@acmecapital.com
# MAGIC **Target:** `production.analytics_bronze.session_analytics` | **Mode:** batch
# MAGIC **Classification:** internal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "production")
SCHEMA = os.getenv("SCHEMA", "analytics_bronze")
TABLE = os.getenv("TABLE", "session_analytics")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# MongoDB: analytics.user_sessions
df = (
    spark.readStream.format("mongodb")
    .option("connection.uri", dbutils.secrets.get("mongo", "uri"))
    .option("database", "analytics")
    .option("collection", "user_sessions")
    .option("change.stream.publish.full.document.only", "true")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (session_analytic_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_session_analytic_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline session_analytics complete -> {TARGET_TABLE}")
