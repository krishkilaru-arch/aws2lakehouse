# Databricks notebook source
# MAGIC %md
# MAGIC # customer_360 - Bronze Layer
# MAGIC **Domain:** customer | **Owner:** customer-platform@acmecapital.com
# MAGIC **Target:** `production.customer_bronze.customer_360` | **Mode:** batch
# MAGIC **Classification:** confidential

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "production")
SCHEMA = os.getenv("SCHEMA", "customer_bronze")
TABLE = os.getenv("TABLE", "customer_360")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# JDBC: public.customers
df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("lending-postgres", "url"))
    .option("user", dbutils.secrets.get("lending-postgres", "username"))
    .option("password", dbutils.secrets.get("lending-postgres", "password"))
    .option("dbtable", "public.customers")
    .option("partitionColumn", "customer_id")
    .option("numPartitions", "20")
    .option("fetchsize", "10000")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (customer_360_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_customer_360_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline customer_360 complete -> {TARGET_TABLE}")
