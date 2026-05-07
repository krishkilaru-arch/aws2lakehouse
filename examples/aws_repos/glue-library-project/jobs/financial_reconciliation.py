"""
financial_reconciliation.py — Match transactions between systems.
Reads from payment gateway (S3) + bank statements (SFTP → S3) + internal ledger (PostgreSQL).
Finds unmatched transactions and generates exception report.

Schedule: Daily at 7:00 AM (after overnight bank file drops)
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from acme_etl.io import read_s3, read_jdbc, write_s3
from acme_etl.transforms import dedup_by_key
from acme_etl.quality import run_quality_checks
from acme_etl.utils import add_audit_columns, audit_log, mask_pii

import time
from pyspark.sql import functions as F
from datetime import date, timedelta

args = getResolvedOptions(sys.argv, ["JOB_NAME", "recon_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

start_time = time.time()
recon_date = args.get("recon_date", str(date.today() - timedelta(days=1)))

# ─── SOURCE 1: Payment gateway transactions (S3 CSV) ───
print(f"Reading payment gateway data for {recon_date}...")
gateway_df = read_s3(spark, {
    "type": "s3",
    "path": f"s3://acme-landing/payment-gateway/{recon_date}/",
    "format": "csv",
    "options": {"header": "true", "inferSchema": "true"},
})
gateway_clean = (
    gateway_df
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("transaction_date", F.to_date("transaction_timestamp"))
    .filter(F.col("status") == "SETTLED")
)

# ─── SOURCE 2: Bank statements (SFTP daily file → S3) ───
print("Reading bank statement data...")
bank_df = read_s3(spark, {
    "type": "s3",
    "path": f"s3://acme-landing/bank-statements/{recon_date}/",
    "format": "csv",
    "options": {"header": "true", "delimiter": "|"},
})
bank_clean = (
    bank_df
    .withColumn("amount", F.col("debit_amount").cast("decimal(18,2)"))
    .withColumn("reference", F.trim(F.col("reference")))
)

# ─── SOURCE 3: Internal ledger (PostgreSQL) ───
print("Reading internal ledger...")
ledger_df = read_jdbc(spark, {
    "secret_name": "prod/finance-postgres",
    "table": f"(SELECT * FROM ledger.transactions WHERE posting_date = '{recon_date}') t",
    "partition_column": "transaction_id",
    "num_partitions": "5",
})

# ─── RECONCILIATION LOGIC ───
print("Running 3-way reconciliation...")

# Match gateway → bank (by reference + amount)
matched_gb = gateway_clean.alias("g").join(
    bank_clean.alias("b"),
    (F.col("g.reference_id") == F.col("b.reference")) &
    (F.abs(F.col("g.amount") - F.col("b.amount")) < 0.01),
    "left"
)

unmatched_gateway = matched_gb.filter(F.col("b.reference").isNull()).select("g.*")

# Match gateway → ledger
matched_gl = gateway_clean.alias("g").join(
    ledger_df.alias("l"),
    (F.col("g.reference_id") == F.col("l.external_ref")) &
    (F.abs(F.col("g.amount") - F.col("l.amount")) < 0.01),
    "left"
)
unmatched_ledger = matched_gl.filter(F.col("l.external_ref").isNull()).select("g.*")

# ─── EXCEPTION REPORT ───
exceptions = (
    unmatched_gateway
    .withColumn("exception_type", F.lit("GATEWAY_NO_BANK_MATCH"))
    .unionByName(
        unmatched_ledger.withColumn("exception_type", F.lit("GATEWAY_NO_LEDGER_MATCH")),
        allowMissingColumns=True
    )
)

# Mask PII before writing exceptions
exceptions_masked = mask_pii(exceptions, [
    {"column": "account_number", "strategy": "partial", "show_chars": 4},
    {"column": "customer_name", "strategy": "tokenize"},
])

exceptions_final = add_audit_columns(exceptions_masked, "financial_reconciliation")

# ─── WRITE ───
write_s3(exceptions_final, {
    "type": "s3",
    "path": f"s3://acme-data-lake/gold/recon_exceptions/{recon_date}/",
    "mode": "overwrite",
})

# ─── QUALITY + AUDIT ───
exception_count = exceptions_final.count()
total_transactions = gateway_clean.count()
match_rate = (total_transactions - exception_count) / total_transactions * 100

audit_log("financial_reconciliation", "SUCCESS", total_transactions,
          time.time() - start_time, {
              "recon_date": recon_date,
              "total_transactions": total_transactions,
              "exceptions": exception_count,
              "match_rate_pct": f"{match_rate:.2f}",
          })

print(f"✅ Reconciliation complete: {match_rate:.1f}% match rate, {exception_count} exceptions")
