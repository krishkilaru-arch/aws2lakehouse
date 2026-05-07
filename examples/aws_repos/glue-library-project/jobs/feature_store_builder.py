"""
feature_store_builder.py — Build ML feature vectors from multiple sources.
Aggregates customer behavior, transaction patterns, product affinity.
Outputs to S3 feature store consumed by SageMaker training jobs.

Schedule: Weekly (Sunday 2:00 AM)
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from acme_etl.io import read_s3, write_s3
from acme_etl.transforms import period_summary, dedup_by_key
from acme_etl.quality import run_quality_checks
from acme_etl.utils import add_audit_columns, generate_hash_key, audit_log

import time
from pyspark.sql import functions as F
from pyspark.sql import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

start_time = time.time()

# ─── Read source data ───
orders = read_s3(spark, {"type": "s3", "path": "s3://acme-data-lake/silver/orders/", "format": "parquet"})
sessions = read_s3(spark, {"type": "s3", "path": "s3://acme-data-lake/silver/web_sessions/", "format": "parquet"})
customer_dim = read_s3(spark, {"type": "s3", "path": "s3://acme-data-lake/gold/customer_dim/", "format": "parquet"})

# ─── Transaction features (last 90 days) ───
print("Building transaction features...")
recent_orders = orders.filter(F.col("order_date") >= F.date_sub(F.current_date(), 90))

txn_features = (
    recent_orders.groupBy("customer_id").agg(
        F.count("order_id").alias("order_count_90d"),
        F.sum("line_total").alias("total_spend_90d"),
        F.avg("line_total").alias("avg_order_value_90d"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("category").alias("distinct_categories_90d"),
        F.stddev("line_total").alias("spend_volatility"),
    )
    .withColumn("days_since_last_order", 
        F.datediff(F.current_date(), F.col("last_order_date")))
    .withColumn("is_high_value", 
        F.when(F.col("total_spend_90d") > 5000, 1).otherwise(0))
)

# ─── Session features ───
print("Building session features...")
recent_sessions = sessions.filter(F.col("session_date") >= F.date_sub(F.current_date(), 30))

session_features = recent_sessions.groupBy("customer_id").agg(
    F.count("session_id").alias("session_count_30d"),
    F.sum("page_views").alias("total_page_views_30d"),
    F.avg("session_duration_min").alias("avg_session_duration"),
    F.sum(F.when(F.col("converted"), 1).otherwise(0)).alias("conversions_30d"),
)

# ─── Join all features ───
print("Joining feature vectors...")
features = (
    customer_dim.filter(F.col("is_current") == True)
    .select("customer_id", "segment", "tenure_months", "credit_score")
    .join(txn_features, "customer_id", "left")
    .join(session_features, "customer_id", "left")
    .na.fill(0)
)

# ─── Quality checks ───
results = run_quality_checks(features, [
    {"type": "not_null", "column": "customer_id"},
    {"type": "unique", "columns": ["customer_id"]},
    {"type": "row_count", "min": 50000},
])

# ─── Write feature store ───
features_final = add_audit_columns(features, "feature_store_builder")
write_s3(features_final, {
    "type": "s3",
    "path": "s3://acme-feature-store/customer_features/latest/",
    "mode": "overwrite",
    "coalesce": 10,
})

# Also write partitioned historical
write_s3(features_final, {
    "type": "s3",
    "path": "s3://acme-feature-store/customer_features/history/",
    "mode": "append",
    "partition_by": ["_batch_id"],
})

duration = time.time() - start_time
audit_log("feature_store_builder", "SUCCESS", features_final.count(), duration)
print(f"✅ Feature store built: {features_final.count()} customer feature vectors in {duration:.0f}s")
