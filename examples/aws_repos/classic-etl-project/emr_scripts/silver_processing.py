"""
silver_processing.py — EMR Spark job: Bronze → Silver layer.
Joins, enriches, and deduplicates data. Builds clean business entities.
"""
import argparse
from pyspark.sql import SparkSession, functions as F, Window

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("EcomCo-Silver").enableHiveSupport().getOrCreate()
process_date = args.date

BASE_BRONZE = f"s3://ecomco-data-lake/bronze"
BASE_SILVER = f"s3://ecomco-data-lake/silver"

# ─── ORDERS (enriched with items + customer) ───
print("Building silver orders...")
orders = spark.read.parquet(f"{BASE_BRONZE}/orders/{process_date}/")
items = spark.read.parquet(f"{BASE_BRONZE}/order_items/{process_date}/")

# Aggregate items to order level
order_items_agg = items.groupBy("order_id").agg(
    F.count("order_item_id").alias("item_count"),
    F.sum("net_amount").alias("items_total"),
    F.collect_set("product_id").alias("product_ids"),
    F.max("category").alias("primary_category"),
)

orders_silver = (
    orders
    .join(order_items_agg, "order_id", "left")
    .withColumn("is_first_order", F.when(
        F.row_number().over(
            Window.partitionBy("customer_id").orderBy("order_date")
        ) == 1, True).otherwise(False))
    .withColumn("days_since_prior", F.datediff(
        F.col("order_date"),
        F.lag("order_date").over(Window.partitionBy("customer_id").orderBy("order_date"))
    ))
)
orders_silver.write.mode("overwrite").partitionBy("order_date").parquet(
    f"{BASE_SILVER}/orders/{process_date}/"
)
print(f"  Silver orders: {orders_silver.count()} rows")

# ─── CLICKSTREAM (sessionized) ───
print("Sessionizing clickstream...")
clicks = spark.read.parquet(f"{BASE_BRONZE}/clickstream/{process_date}/")

# Session aggregation
session_window = Window.partitionBy("session_id").orderBy("event_timestamp")
sessions_silver = (
    clicks
    .groupBy("session_id", "user_id", "device_type", "country")
    .agg(
        F.min("event_timestamp").alias("session_start"),
        F.max("event_timestamp").alias("session_end"),
        F.count("event_id").alias("page_views"),
        F.countDistinct("product_id").alias("products_viewed"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.first("referrer").alias("entry_referrer"),
    )
    .withColumn("session_duration_min", 
        (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 60)
    .withColumn("converted", F.col("purchases") > 0)
    .withColumn("bounce", F.col("page_views") == 1)
)
sessions_silver.write.mode("overwrite").parquet(f"{BASE_SILVER}/sessions/{process_date}/")
print(f"  Silver sessions: {sessions_silver.count()} rows")

# ─── CUSTOMER 360 (merge CRM + behavior) ───
print("Building customer 360...")
# Read existing customer dim
try:
    existing_customers = spark.read.parquet(f"{BASE_SILVER}/customer_360/")
except:
    existing_customers = spark.createDataFrame([], orders.schema)

# Aggregate behavior
customer_behavior = (
    orders_silver
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("lifetime_orders"),
        F.sum("order_total").alias("lifetime_value"),
        F.avg("order_total").alias("avg_order_value"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date"),
    )
    .withColumn("customer_tenure_days", 
        F.datediff(F.current_date(), F.col("first_order_date")))
    .withColumn("rfm_recency", F.datediff(F.current_date(), F.col("last_order_date")))
)

customer_behavior.write.mode("overwrite").parquet(f"{BASE_SILVER}/customer_360/{process_date}/")
print(f"  Customer 360: {customer_behavior.count()} rows")

print(f"\n✅ Silver layer complete for {process_date}")
spark.stop()
