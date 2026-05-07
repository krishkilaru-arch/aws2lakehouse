# Databricks notebook source
# MAGIC %md
# MAGIC # loan_application_etl - Bronze Layer
# MAGIC **Domain:** lending | **Owner:** lending-team@company.com
# MAGIC **Target:** `acme_prod.lending_bronze.loan_application_etl` | **Mode:** batch
# MAGIC **Classification:** confidential

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "lending_bronze")
TABLE = os.getenv("TABLE", "loan_application_etl")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# JDBC: loan_application_etl
df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("jdbc", "url"))
    .option("user", dbutils.secrets.get("jdbc", "username"))
    .option("password", dbutils.secrets.get("jdbc", "password"))
    .option("dbtable", "loan_application_etl")
    .option("partitionColumn", "id")
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
