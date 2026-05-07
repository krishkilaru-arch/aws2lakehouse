# Databricks notebook source
# MAGIC %md
# MAGIC # nav_calculation_job - Bronze Layer
# MAGIC **Domain:** finance | **Owner:** data-team@company.com
# MAGIC **Target:** `acme_prod.finance_bronze.nav_calculation_job` | **Mode:** batch
# MAGIC **Classification:** internal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "finance_bronze")
TABLE = os.getenv("TABLE", "nav_calculation_job")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# JDBC: nav_calculation_job
df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("jdbc", "url"))
    .option("user", dbutils.secrets.get("jdbc", "username"))
    .option("password", dbutils.secrets.get("jdbc", "password"))
    .option("dbtable", "nav_calculation_job")
    .option("partitionColumn", "id")
    .option("numPartitions", "20")
    .option("fetchsize", "10000")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (nav_calculation_job_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_nav_calculation_job_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline nav_calculation_job complete -> {TARGET_TABLE}")
