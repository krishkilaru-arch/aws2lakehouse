# Databricks notebook source
# MAGIC %md
# MAGIC # customer_churn_model - Bronze Layer
# MAGIC **Domain:** analytics | **Owner:** data-team@company.com
# MAGIC **Target:** `acme_prod.analytics_bronze.customer_churn_model` | **Mode:** batch
# MAGIC **Classification:** internal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "analytics_bronze")
TABLE = os.getenv("TABLE", "customer_churn_model")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

df = spark.table("")

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (customer_churn_model_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_customer_churn_model_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline customer_churn_model complete -> {TARGET_TABLE}")
