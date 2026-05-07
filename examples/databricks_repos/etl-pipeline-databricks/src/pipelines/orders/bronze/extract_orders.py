# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Orders — Bronze Layer
# MAGIC **Source:** PostgreSQL (orders_replica) via JDBC
# MAGIC **Schedule:** Daily at 2 AM UTC
# MAGIC **Replaces:** Airflow `extract_orders` task + custom Python script

# COMMAND ----------

from pyspark.sql import functions as F
import os

CATALOG = os.getenv("CATALOG", "production")
TARGET = f"{CATALOG}.orders_bronze.raw_orders"

# COMMAND ----------

# JDBC extract with parallel partitioned reads (replaces PostgresExtractor)
orders_df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("shopmax-orders-db", "jdbc_url"))
    .option("user", dbutils.secrets.get("shopmax-orders-db", "username"))
    .option("password", dbutils.secrets.get("shopmax-orders-db", "password"))
    .option("dbtable", "public.orders")
    .option("partitionColumn", "order_id")
    .option("numPartitions", "20")
    .option("fetchsize", "50000")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Write to Delta (append for daily snapshots, merge for idempotency)
orders_df.write.format("delta").mode("append").saveAsTable(TARGET)
print(f"✅ Extracted {orders_df.count()} orders → {TARGET}")

