"""
Revenue Aggregation — Calculate daily/weekly/monthly revenue metrics.

Gold layer: Revenue by merchant, category, geography, channel.
"""
import sys
import argparse
from pyspark.sql import SparkSession, functions as F, Window


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName(f"RevenueAgg-{args.date}").getOrCreate()
    
    enriched = spark.read.parquet(args.input)
    
    # ─── DAILY REVENUE BY DIMENSIONS ───
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
        .withColumn("revenue_per_customer", F.col("net_revenue") / F.col("unique_customers"))
    )
    
    # ─── ROLLING METRICS (7-day, 30-day) ───
    window_7d = Window.partitionBy("category").orderBy("order_date").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("category").orderBy("order_date").rowsBetween(-29, 0)
    
    daily_revenue = (
        daily_revenue
        .withColumn("revenue_7d_avg", F.avg("net_revenue").over(window_7d))
        .withColumn("revenue_30d_avg", F.avg("net_revenue").over(window_30d))
        .withColumn("order_count_7d_avg", F.avg("order_count").over(window_7d))
    )
    
    # ─── WRITE ───
    daily_revenue.write.mode("overwrite").parquet(args.output)
    print(f"✅ Revenue aggregation: {daily_revenue.count()} metric rows for {args.date}")
    spark.stop()


if __name__ == "__main__":
    main()
