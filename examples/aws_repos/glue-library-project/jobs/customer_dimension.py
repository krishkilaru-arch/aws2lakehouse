"""
customer_dimension.py — Glue Job: SCD2 customer dimension build.
Runs daily, reads from PostgreSQL CRM, merges into customer_dim on S3.

Deploy: aws glue create-job --name customer_dimension --extra-py-files s3://acme-libs/acme_etl-3.2.1-py3-none-any.whl
Schedule: Daily at 2:00 AM ET (Glue Trigger)
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from acme_etl.io import read_jdbc
from acme_etl.transforms import scd2_merge, dedup_by_key
from acme_etl.quality import run_quality_checks
from acme_etl.utils import mask_pii, add_audit_columns, audit_log

import time

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

start_time = time.time()

# ─── READ ───
print("Reading from CRM PostgreSQL...")
customers_raw = read_jdbc(spark, {
    "secret_name": "prod/crm-postgres",
    "table": "public.customers",
    "partition_column": "customer_id",
    "num_partitions": "10",
})

# ─── DEDUP ───
print("Deduplicating by customer_id (keep latest)...")
customers_deduped = dedup_by_key(customers_raw, ["customer_id"], "updated_at")

# ─── QUALITY ───
print("Running quality checks...")
results = run_quality_checks(customers_deduped, [
    {"type": "not_null", "column": "customer_id"},
    {"type": "not_null", "column": "email"},
    {"type": "unique", "columns": ["customer_id"]},
    {"type": "row_count", "min": 100000, "max": 50000000},
    {"type": "range", "column": "credit_score", "min": 300, "max": 850},
])
failed = [r for r in results if not r.passed]
if failed:
    print(f"WARNING: {len(failed)} quality checks failed: {[r.check_name for r in failed]}")

# ─── MASK PII for silver zone ───
customers_masked = mask_pii(customers_deduped, [
    {"column": "ssn", "strategy": "hash"},
    {"column": "email", "strategy": "partial", "show_chars": 3},
    {"column": "phone", "strategy": "redact"},
    {"column": "date_of_birth", "strategy": "year_only"},
])

# ─── SCD2 MERGE ───
print("Running SCD2 merge...")
merge_result = scd2_merge(
    source_df=customers_masked,
    target_table="s3://acme-data-lake/gold/customer_dim/",
    key_cols=["customer_id"],
    compare_cols=["name", "address", "city", "state", "zip", "credit_score", "segment", "status"],
    spark_session=spark,
)
print(f"SCD2 result: {merge_result}")

# ─── AUDIT ───
duration = time.time() - start_time
audit_log("customer_dimension", "SUCCESS", 
          merge_result["inserted"] + merge_result["updated"],
          duration, merge_result)

print(f"✅ customer_dimension complete in {duration:.0f}s")
