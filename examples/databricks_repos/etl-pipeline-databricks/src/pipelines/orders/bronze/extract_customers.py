# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Customers — Bronze (incremental via watermark)

# COMMAND ----------

from pyspark.sql import functions as F
import os

CATALOG = os.getenv("CATALOG", "production")
TARGET = f"{CATALOG}.orders_bronze.raw_customers"

# Get last watermark from Delta table history
try:
    last_ts = spark.sql(f"SELECT MAX(updated_at) FROM {TARGET}").collect()[0][0]
except:
    last_ts = "2020-01-01 00:00:00"

customers_df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("shopmax-customers-db", "jdbc_url"))
    .option("user", dbutils.secrets.get("shopmax-customers-db", "username"))
    .option("password", dbutils.secrets.get("shopmax-customers-db", "password"))
    .option("dbtable", f"(SELECT * FROM public.customers WHERE updated_at > '{last_ts}') t")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

# MERGE (upsert) for incremental
customers_df.createOrReplaceTempView("new_customers")
spark.sql(f"""
    MERGE INTO {TARGET} t USING new_customers s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"✅ Customers: {customers_df.count()} incremental rows merged → {TARGET}")

