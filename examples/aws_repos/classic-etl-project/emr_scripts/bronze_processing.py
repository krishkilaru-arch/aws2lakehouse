"""
bronze_processing.py — EMR Spark job: Raw → Bronze layer.
Reads from S3 staging (extracts), applies basic cleansing, writes to Bronze zone.
"""
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("EcomCo-Bronze").enableHiveSupport().getOrCreate()
process_date = args.date

BASE_STAGING = f"s3://ecomco-staging/extract"
BASE_BRONZE = f"s3://ecomco-data-lake/bronze"

# ─── Orders (from PostgreSQL extract) ───
print(f"Processing orders for {process_date}...")
orders_raw = spark.read.parquet(f"{BASE_STAGING}/orders/{process_date}/orders/")
orders_bronze = (
    orders_raw
    .withColumn("order_date", F.to_date("created_at"))
    .withColumn("order_total", F.col("subtotal") + F.col("tax") + F.col("shipping"))
    .withColumn("_source", F.lit("postgres_orders"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_process_date", F.lit(process_date))
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)
orders_bronze.write.mode("overwrite").partitionBy("order_date").parquet(
    f"{BASE_BRONZE}/orders/{process_date}/"
)
print(f"  Orders: {orders_bronze.count()} rows")

# ─── Order Items ───
items_raw = spark.read.parquet(f"{BASE_STAGING}/orders/{process_date}/order_items/")
items_bronze = (
    items_raw
    .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    .withColumn("discount_amount", F.col("line_total") * F.coalesce(F.col("discount_pct"), F.lit(0)))
    .withColumn("net_amount", F.col("line_total") - F.col("discount_amount"))
    .withColumn("_ingested_at", F.current_timestamp())
    .dropDuplicates(["order_item_id"])
)
items_bronze.write.mode("overwrite").parquet(f"{BASE_BRONZE}/order_items/{process_date}/")
print(f"  Order Items: {items_bronze.count()} rows")

# ─── Inventory snapshots (from MySQL) ───
inv_raw = spark.read.parquet(f"{BASE_STAGING}/inventory/{process_date}/inventory_snapshots/")
inv_bronze = (
    inv_raw
    .withColumn("is_low_stock", F.col("quantity_available") < F.col("reorder_point"))
    .withColumn("_ingested_at", F.current_timestamp())
)
inv_bronze.write.mode("overwrite").parquet(f"{BASE_BRONZE}/inventory/{process_date}/")
print(f"  Inventory: {inv_bronze.count()} rows")

# ─── Clickstream (raw JSON logs) ───
click_raw = spark.read.json(f"s3://ecomco-raw-data/clickstream/{process_date}/")
click_bronze = (
    click_raw
    .select(
        F.col("event_id"),
        F.col("user_id"),
        F.col("session_id"),
        F.col("event_type"),
        F.col("page_url"),
        F.col("product_id"),
        F.to_timestamp("timestamp").alias("event_timestamp"),
        F.col("device_type"),
        F.col("geo.country").alias("country"),
        F.col("geo.city").alias("city"),
        F.col("referrer"),
    )
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("_ingested_at", F.current_timestamp())
    .filter(F.col("event_id").isNotNull())
)
click_bronze.write.mode("overwrite").partitionBy("event_date").parquet(
    f"{BASE_BRONZE}/clickstream/{process_date}/"
)
print(f"  Clickstream: {click_bronze.count()} rows")

# ─── Marketing attribution ───
mktg_raw = spark.read.csv(
    f"{BASE_STAGING}/marketing/{process_date}/", header=True, inferSchema=True
)
mktg_bronze = (
    mktg_raw
    .withColumn("attribution_date", F.to_date("click_date"))
    .withColumn("cost", F.col("spend").cast("decimal(10,2)"))
    .withColumn("_ingested_at", F.current_timestamp())
)
mktg_bronze.write.mode("overwrite").parquet(f"{BASE_BRONZE}/marketing/{process_date}/")
print(f"  Marketing: {mktg_bronze.count()} rows")

print(f"\n✅ Bronze layer complete for {process_date}")
spark.stop()
