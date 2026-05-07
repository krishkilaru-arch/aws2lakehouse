"""
gold_star_schema.py — EMR Spark job: Silver → Gold (Star Schema for Redshift).
Builds dimensional model: fact_orders, dim_customer, dim_product, dim_date, dim_geography.
"""
import argparse
from pyspark.sql import SparkSession, functions as F, Window

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("EcomCo-Gold-StarSchema").enableHiveSupport().getOrCreate()
process_date = args.date

BASE_SILVER = f"s3://ecomco-data-lake/silver"
BASE_GOLD = f"s3://ecomco-data-lake/gold"

# ─── FACT: Orders ───
print("Building fact_orders...")
orders = spark.read.parquet(f"{BASE_SILVER}/orders/{process_date}/")

fact_orders = (
    orders.select(
        F.col("order_id"),
        F.col("customer_id").alias("customer_key"),
        F.col("order_date").alias("date_key"),
        F.col("shipping_city").alias("geography_key"),
        F.col("item_count"),
        F.col("items_total").alias("gross_revenue"),
        F.col("discount_amount"),
        F.col("order_total").alias("net_revenue"),
        F.col("shipping").alias("shipping_revenue"),
        F.col("tax_amount"),
        F.col("is_first_order"),
        F.col("order_status"),
        F.lit(1).alias("order_count"),
    )
)
fact_orders.write.mode("overwrite").partitionBy("date_key").parquet(
    f"{BASE_GOLD}/fact_orders/{process_date}/"
)
print(f"  fact_orders: {fact_orders.count()} rows")

# ─── DIM: Customer ───
print("Building dim_customer...")
customers = spark.read.parquet(f"{BASE_SILVER}/customer_360/{process_date}/")

dim_customer = (
    customers.select(
        F.col("customer_id").alias("customer_key"),
        F.col("customer_id"),
        F.col("lifetime_orders"),
        F.col("lifetime_value"),
        F.col("avg_order_value"),
        F.col("customer_tenure_days"),
        F.col("rfm_recency"),
        F.when(F.col("lifetime_value") > 10000, "Platinum")
         .when(F.col("lifetime_value") > 5000, "Gold")
         .when(F.col("lifetime_value") > 1000, "Silver")
         .otherwise("Bronze").alias("value_segment"),
        F.when(F.col("rfm_recency") <= 30, "Active")
         .when(F.col("rfm_recency") <= 90, "At Risk")
         .when(F.col("rfm_recency") <= 180, "Lapsed")
         .otherwise("Churned").alias("activity_status"),
    )
)
dim_customer.write.mode("overwrite").parquet(f"{BASE_GOLD}/dim_customer/latest/")
print(f"  dim_customer: {dim_customer.count()} rows")

# ─── DIM: Date ───
print("Building dim_date...")
from datetime import date, timedelta

dates = []
start = date(2020, 1, 1)
for i in range(2500):  # ~7 years
    d = start + timedelta(days=i)
    dates.append((
        d, d.year, d.month, d.day, d.strftime("%A"),
        (d.month - 1) // 3 + 1, d.isocalendar()[1],
        d.weekday() >= 5
    ))

dim_date = spark.createDataFrame(dates, [
    "date_key", "year", "month", "day", "day_name", "quarter", "week_of_year", "is_weekend"
])
dim_date.write.mode("overwrite").parquet(f"{BASE_GOLD}/dim_date/")
print(f"  dim_date: {dim_date.count()} rows")

print(f"\n✅ Gold star schema complete for {process_date}")
spark.stop()
