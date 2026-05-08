"""
position_snapshot_job.py — Glue Job: Daily position snapshot from OMS (PostgreSQL).

Schedule: Daily at 8:00 AM ET
Source: PostgreSQL (OMS - Order Management System)
Target: s3://acme-data-lake/bronze/positions/
Library: acme_datalib
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

from acme_datalib.connectors.database import DatabaseConnector
from acme_datalib.transformations.positions import calculate_pnl, reconcile_with_custodian
from acme_datalib.quality.validators import DataValidator
from acme_datalib.utils.logging_config import setup_logging
from acme_datalib.utils.encryption import FieldEncryptor

args = getResolvedOptions(sys.argv, ["JOB_NAME", "snapshot_date", "environment"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

snapshot_date = args.get("snapshot_date", datetime.now().strftime("%Y-%m-%d"))
logger = setup_logging("position_snapshot", job_name=args["JOB_NAME"])

# ─── EXTRACT from OMS ───
logger.info(f"Extracting positions from OMS for {snapshot_date}")
oms_db = DatabaseConnector(spark, secret_name="prod/oms-postgres")
positions_df = oms_db.read_table(
    table=f"(SELECT * FROM positions WHERE position_date = '{snapshot_date}') t",
    partition_column="account_id",
    num_partitions=20
)
logger.info(f"Extracted {positions_df.count()} positions")

# ─── EXTRACT from Custodian ───
logger.info("Extracting custodian positions for reconciliation")
custodian_db = DatabaseConnector(spark, secret_name="prod/statestreet-sftp-db")
custodian_df = custodian_db.read_table(
    table=f"(SELECT * FROM custodian_positions WHERE as_of_date = '{snapshot_date}') t"
)

# ─── RECONCILE ───
logger.info("Running position reconciliation")
recon_df = reconcile_with_custodian(positions_df, custodian_df)
breaks = recon_df.filter(F.col("break_type") != "MATCHED")
break_count = breaks.count()

if break_count > 0:
    logger.warning(f"⚠️ {break_count} reconciliation breaks detected")
    breaks.write.mode("overwrite").parquet(
        f"s3://acme-data-lake/recon/breaks/date={snapshot_date}/"
    )

# ─── ENCRYPT PII ───
encryptor = FieldEncryptor(kms_key_id="arn:aws:kms:us-east-1:123456789:key/abc-123")
encrypted_df = encryptor.encrypt_columns(positions_df, ["account_holder_name", "tax_id"])

# ─── VALIDATE ───
validator = DataValidator(encrypted_df, "position_snapshot")
validator.check_not_null(["account_id", "instrument_id", "quantity", "position_date"])
validator.check_range("quantity", min_val=-1_000_000_000, max_val=1_000_000_000)
validator.validate(fail_on_error=True)

# ─── LOAD ───
output_path = f"s3://acme-data-lake/bronze/positions/position_date={snapshot_date}/"
encrypted_df.write.mode("overwrite").parquet(output_path)

job.commit()
logger.info(f"✅ Position snapshot complete: {encrypted_df.count()} positions, {break_count} breaks")
