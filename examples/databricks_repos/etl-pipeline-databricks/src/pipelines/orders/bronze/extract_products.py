# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Products — Bronze (JDBC from product catalog DB)

# COMMAND ----------

from pyspark.sql import functions as F
import os

CATALOG = os.getenv("CATALOG", "production")
TARGET = f"{CATALOG}.orders_bronze.raw_products"

products_df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("shopmax-products-db", "jdbc_url"))
    .option("user", dbutils.secrets.get("shopmax-products-db", "username"))
    .option("password", dbutils.secrets.get("shopmax-products-db", "password"))
    .option("dbtable", "public.products")
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
)

products_df.write.format("delta").mode("overwrite").saveAsTable(TARGET)
print(f"✅ Products: {products_df.count()} rows → {TARGET}")

