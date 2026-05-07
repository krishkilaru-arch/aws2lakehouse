# Databricks notebook source
# MAGIC %md
# MAGIC # compliance_report - Bronze Layer
# MAGIC **Domain:** compliance | **Owner:** compliance-team@acmecapital.com
# MAGIC **Target:** `production.compliance_bronze.compliance_report` | **Mode:** batch
# MAGIC **Classification:** confidential

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "production")
SCHEMA = os.getenv("SCHEMA", "compliance_bronze")
TABLE = os.getenv("TABLE", "compliance_report")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

df = spark.table("loan_applications")

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (compliance_report_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_compliance_report_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline compliance_report complete -> {TARGET_TABLE}")
