"""
vendor_data_ingestion.py — Ingest files from multiple vendors in different formats.
Each vendor drops files in their own S3 prefix with different formats/schemas.
This job normalizes them all into a standard schema.

Schedule: Every 15 minutes (event-triggered by S3 notifications)
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from acme_etl.io import read_s3, write_s3
from acme_etl.transforms import flatten_nested, dedup_by_key
from acme_etl.quality import run_quality_checks, validate_schema
from acme_etl.utils import add_audit_columns, generate_hash_key, audit_log

import time
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "vendor_name", "input_path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

start_time = time.time()
vendor = args["vendor_name"]
input_path = args["input_path"]

# Vendor-specific configs
VENDOR_CONFIGS = {
    "bloomberg": {"format": "json", "id_field": "figi", "date_field": "pricing_date"},
    "refinitiv": {"format": "csv", "id_field": "ric_code", "date_field": "trade_date", "delimiter": "|"},
    "ice": {"format": "parquet", "id_field": "instrument_id", "date_field": "valuation_date"},
    "markit": {"format": "xml", "id_field": "red_code", "date_field": "as_of_date"},
    "internal": {"format": "json", "id_field": "internal_id", "date_field": "effective_date"},
}

config = VENDOR_CONFIGS.get(vendor)
if not config:
    raise ValueError(f"Unknown vendor: {vendor}. Known: {list(VENDOR_CONFIGS.keys())}")

# ─── READ vendor file ───
print(f"Reading {vendor} data from {input_path} (format: {config['format']})...")
options = {}
if config["format"] == "csv":
    options = {"header": "true", "inferSchema": "true", "delimiter": config.get("delimiter", ",")}

raw_df = read_s3(spark, {"type": "s3", "path": input_path, "format": config["format"], "options": options})

# ─── FLATTEN if nested ───
if config["format"] == "json":
    raw_df = flatten_nested(raw_df)

# ─── NORMALIZE to standard schema ───
normalized = (
    raw_df
    .withColumnRenamed(config["id_field"], "instrument_id")
    .withColumnRenamed(config["date_field"], "effective_date")
    .withColumn("vendor", F.lit(vendor))
    .withColumn("effective_date", F.to_date("effective_date"))
)

# ─── DEDUP (vendors sometimes send duplicates) ───
normalized = dedup_by_key(normalized, ["instrument_id", "effective_date"], "effective_date")

# ─── HASH KEY ───
normalized = generate_hash_key(normalized, ["instrument_id", "effective_date", "vendor"])

# ─── QUALITY ───
results = run_quality_checks(normalized, [
    {"type": "not_null", "column": "instrument_id"},
    {"type": "not_null", "column": "effective_date"},
    {"type": "row_count", "min": 1},
])

# ─── WRITE ───
final = add_audit_columns(normalized, f"vendor_{vendor}_ingestion")
write_s3(final, {
    "type": "s3",
    "path": f"s3://acme-data-lake/bronze/vendor_data/{vendor}/",
    "mode": "append",
    "partition_by": ["effective_date"],
})

duration = time.time() - start_time
audit_log(f"vendor_{vendor}_ingestion", "SUCCESS", final.count(), duration)
print(f"✅ {vendor} ingestion complete: {final.count()} records in {duration:.0f}s")
