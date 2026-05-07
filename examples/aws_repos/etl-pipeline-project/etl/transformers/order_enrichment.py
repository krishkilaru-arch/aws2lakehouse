"""
Order Enrichment — Join orders with products and customers, calculate metrics.

This is the main EMR spark-submit job referenced by the Airflow DAG.
"""
import sys
import argparse
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--input-orders", required=True)
    parser.add_argument("--input-products", required=True)
    parser.add_argument("--input-customers", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder         .appName(f"OrderEnrichment-{args.date}")         .config("spark.sql.adaptive.enabled", "true")         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")         .getOrCreate()
    
    # ─── READ INPUTS ───
    orders = spark.read.json(args.input_orders)
    products = spark.read.parquet(args.input_products)
    customers = spark.read.parquet(args.input_customers)
    
    print(f"Orders: {orders.count()}, Products: {products.count()}, Customers: {customers.count()}")
    
    # ─── EXPLODE ORDER ITEMS ───
    order_items = (
        orders
        .withColumn("item", F.explode("line_items"))
        .select(
            "order_id", "customer_id", "order_date", "status",
            "shipping_address.*",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.unit_price").alias("unit_price"),
            F.col("item.discount_amount").alias("discount"),
        )
        .withColumn("line_total", F.col("quantity") * F.col("unit_price") - F.col("discount"))
    )
    
    # ─── ENRICH WITH PRODUCT DATA ───
    enriched = (
        order_items
        .join(products.select("product_id", "category", "brand", "supplier_id", "cost_price"),
              "product_id", "left")
        .join(customers.select("customer_id", "segment", "lifetime_value", "first_order_date",
                              "country", "acquisition_channel"),
              "customer_id", "left")
    )
    
    # ─── CALCULATE METRICS ───
    enriched = (
        enriched
        .withColumn("margin", F.col("unit_price") - F.coalesce(F.col("cost_price"), F.lit(0)))
        .withColumn("margin_pct", F.col("margin") / F.col("unit_price") * 100)
        .withColumn("is_first_order", F.col("order_date") == F.col("first_order_date"))
        .withColumn("days_since_first_order",
            F.datediff(F.col("order_date"), F.col("first_order_date")))
        .withColumn("order_cohort_month",
            F.date_format(F.col("first_order_date"), "yyyy-MM"))
    )
    
    # ─── WRITE ───
    enriched.write         .mode("overwrite")         .partitionBy("category")         .parquet(args.output)
    
    print(f"✅ Wrote {enriched.count()} enriched order items to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
