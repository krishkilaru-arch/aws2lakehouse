# Databricks notebook source
# MAGIC %md
# MAGIC # Enrich Orders — Silver Layer
# MAGIC **Replaces:** EMR spark-submit `enrich_orders.py` (16 executors, 16g memory)
# MAGIC **Now:** Runs on Databricks Job cluster with Photon (3x faster, auto-tuned)

# COMMAND ----------

from pyspark.sql import functions as F
import os

CATALOG = os.getenv("CATALOG", "production")

# Read from Bronze Delta tables (instant, no S3 scanning!)
orders = spark.table(f"{CATALOG}.orders_bronze.raw_orders")
products = spark.table(f"{CATALOG}.orders_bronze.raw_products")
customers = spark.table(f"{CATALOG}.orders_bronze.raw_customers")

print(f"Orders: {orders.count():,}, Products: {products.count():,}, Customers: {customers.count():,}")

# COMMAND ----------

# Explode order line items (same logic as EMR version)
order_items = (
    orders
    .withColumn("item", F.explode("line_items"))
    .select(
        "order_id", "customer_id", "order_date", "status",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price"),
        F.col("item.discount_amount").alias("discount"),
    )
    .withColumn("line_total", F.col("quantity") * F.col("unit_price") - F.col("discount"))
)

# COMMAND ----------

# Enrich with product + customer data
enriched = (
    order_items
    .join(products.select("product_id", "category", "brand", "supplier_id", "cost_price"),
          "product_id", "left")
    .join(customers.select("customer_id", "segment", "lifetime_value", "first_order_date",
                          "country", "acquisition_channel"),
          "customer_id", "left")
    .withColumn("margin", F.col("unit_price") - F.coalesce(F.col("cost_price"), F.lit(0)))
    .withColumn("margin_pct", F.col("margin") / F.col("unit_price") * 100)
    .withColumn("is_first_order", F.col("order_date") == F.col("first_order_date"))
    .withColumn("order_cohort_month", F.date_format(F.col("first_order_date"), "yyyy-MM"))
    .withColumn("_enriched_at", F.current_timestamp())
)

# COMMAND ----------

# Write to Silver (Delta Lake with Z-ORDER for query performance)
enriched.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.orders_silver.enriched_orders")
spark.sql(f"OPTIMIZE {CATALOG}.orders_silver.enriched_orders ZORDER BY (order_date, category)")
print(f"✅ Enriched orders: {enriched.count():,} rows")

