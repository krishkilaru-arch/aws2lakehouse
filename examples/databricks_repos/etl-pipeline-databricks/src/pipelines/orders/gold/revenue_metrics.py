# Databricks notebook source
# MAGIC %md
# MAGIC # Revenue Metrics — Gold Layer
# MAGIC **Replaces:** EMR aggregate_revenue.py + Redshift COPY load
# MAGIC **Now:** Direct Delta Lake Gold table (no separate warehouse needed!)

# COMMAND ----------

from pyspark.sql import functions as F, Window
import os

CATALOG = os.getenv("CATALOG", "production")
enriched = spark.table(f"{CATALOG}.orders_silver.enriched_orders")

# COMMAND ----------

# Daily revenue by dimensions
daily_revenue = (
    enriched
    .groupBy("order_date", "category", "brand", "segment", "country", "acquisition_channel")
    .agg(
        F.sum("line_total").alias("gross_revenue"),
        F.sum("discount").alias("total_discounts"),
        F.sum("margin").alias("total_margin"),
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("units_sold"),
        F.avg("margin_pct").alias("avg_margin_pct"),
    )
    .withColumn("net_revenue", F.col("gross_revenue") - F.col("total_discounts"))
    .withColumn("revenue_per_order", F.col("net_revenue") / F.col("order_count"))
    .withColumn("_calculated_at", F.current_timestamp())
)

# COMMAND ----------

# Rolling windows (7d, 30d)
w7 = Window.partitionBy("category").orderBy("order_date").rowsBetween(-6, 0)
w30 = Window.partitionBy("category").orderBy("order_date").rowsBetween(-29, 0)

daily_revenue = (
    daily_revenue
    .withColumn("revenue_7d_avg", F.avg("net_revenue").over(w7))
    .withColumn("revenue_30d_avg", F.avg("net_revenue").over(w30))
)

daily_revenue.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.analytics_gold.daily_revenue")
print(f"✅ Revenue metrics: {daily_revenue.count():,} rows")

