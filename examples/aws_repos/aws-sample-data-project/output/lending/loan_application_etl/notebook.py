# Databricks notebook source
# MAGIC %md
# MAGIC # loan_application_etl - Bronze Layer
# MAGIC **Domain:** lending | **Owner:** lending-team@acmecapital.com
# MAGIC **Target:** `production.lending_bronze.loan_application_etl` | **Mode:** batch
# MAGIC **Classification:** confidential

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "production")
SCHEMA = os.getenv("SCHEMA", "lending_bronze")
TABLE = os.getenv("TABLE", "loan_application_etl")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# JDBC: public.loan_applications
df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("lending-postgres", "url"))
    .option("user", dbutils.secrets.get("lending-postgres", "username"))
    .option("password", dbutils.secrets.get("lending-postgres", "password"))
    .option("dbtable", "public.loan_applications")
    .option("partitionColumn", "application_id")
    .option("numPartitions", "20")
    .option("fetchsize", "10000")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (loan_application_etl_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_loan_application_etl_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline loan_application_etl complete -> {TARGET_TABLE}")
