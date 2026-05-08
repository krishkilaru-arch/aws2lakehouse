"""
order_events_processing.py — Process order events from S3 landing zone.
Flatten nested JSON, enrich with product dim, write to silver + gold.

Schedule: Hourly (Glue Trigger, on S3 landing detection)
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from acme_etl.io import read_s3, write_s3
from acme_etl.transforms import flatten_nested, explode_arrays, lookup_join, running_total
from acme_etl.quality import run_quality_checks, validate_schema
from acme_etl.utils import add_audit_columns, generate_hash_key, audit_log

import time
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

start_time = time.time()
input_path = args["input_path"]  # s3://acme-landing/orders/
output_path = args["output_path"]  # s3://acme-data-lake/silver/orders/

# ─── READ (nested JSON from event stream) ───
print(f"Reading events from {input_path}...")
raw_events = read_s3(spark, {
    "type": "s3", "path": input_path, "format": "json",
    "options": {"multiLine": "true"}
})

# ─── FLATTEN nested structures ───
print("Flattening nested JSON...")
flat_events = flatten_nested(raw_events)

# ─── EXPLODE order items array ───
if "items" in [f.name for f in flat_events.schema.fields]:
    flat_events = explode_arrays(flat_events, ["items"])

# ─── GENERATE hash key ───
flat_events = generate_hash_key(flat_events, ["order_id", "item_id"], "order_item_key")

# ─── SCHEMA VALIDATION ───
schema_results = validate_schema(flat_events, {
    "order_id": "string",
    "customer_id": "string",
    "order_item_key": "string",
})
schema_failures = [r for r in schema_results if not r.passed]
if schema_failures:
    print(f"⚠️ Schema issues: {[r.details for r in schema_failures]}")

# ─── ENRICH with product dimension ───
print("Enriching with product reference data...")
products = spark.read.parquet("s3://acme-reference-data/products/")
enriched = lookup_join(
    flat_events, products, "product_id",
    select_cols=["product_name", "category", "brand", "unit_cost"],
    default_values={"category": "UNKNOWN", "brand": "UNKNOWN"}
)

# ─── CALCULATE running totals ───
enriched = enriched.withColumn("line_total", F.col("quantity") * F.col("unit_price"))
with_running = running_total(enriched, "customer_id", "order_date", "line_total")

# ─── QUALITY CHECKS ───
results = run_quality_checks(with_running, [
    {"type": "not_null", "column": "order_id"},
    {"type": "not_null", "column": "customer_id"},
    {"type": "range", "column": "line_total", "min": 0, "max": 500000},
    {"type": "row_count", "min": 100},
])

# ─── ADD AUDIT COLUMNS ───
final = add_audit_columns(with_running, "order_events_processing")

# ─── WRITE to silver ───
print(f"Writing to {output_path}...")
write_s3(final, {
    "type": "s3", "path": output_path, "mode": "append",
    "partition_by": ["order_date"],
})

duration = time.time() - start_time
audit_log("order_events_processing", "SUCCESS", final.count(), duration)
print(f"✅ order_events_processing complete: {final.count()} records in {duration:.0f}s")
