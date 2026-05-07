# Databricks notebook source
# MAGIC %md
# MAGIC # customer_dimension - Bronze Layer
# MAGIC **Domain:** finance | **Owner:** data-team@company.com
# MAGIC **Target:** `acme_prod.finance_bronze.customer_dimension` | **Mode:** batch
# MAGIC **Classification:** confidential

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

CATALOG = os.getenv("CATALOG", "acme_prod")
SCHEMA = os.getenv("SCHEMA", "finance_bronze")
TABLE = os.getenv("TABLE", "customer_dimension")
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# JDBC: customer_dimension
df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("jdbc", "url"))
    .option("user", dbutils.secrets.get("jdbc", "username"))
    .option("password", dbutils.secrets.get("jdbc", "password"))
    .option("dbtable", "customer_dimension")
    .option("partitionColumn", "id")
    .option("numPartitions", "20")
    .option("fetchsize", "10000")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Data Quality Checks
assert df.filter("NOT (customer_dimension_id IS NOT NULL)").count() == 0, "DQ FAIL: valid_customer_dimension_id"

# COMMAND ----------

# Write: Batch append
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------
print(f"Pipeline customer_dimension complete -> {TARGET_TABLE}")
