"""
nav_calculation_job.py — Glue Job: Calculate fund NAV for investor reporting.

Schedule: Daily at 10:00 AM ET (after risk aggregation)
Source: Silver layer (risk_metrics, cash_balances)
Target: s3://acme-data-lake/gold/nav/
Library: acme_datalib
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

from acme_datalib.transformations.nav import calculate_fund_nav, calculate_performance_attribution
from acme_datalib.quality.validators import DataValidator
from acme_datalib.quality.reconciliation import ReconciliationEngine
from acme_datalib.utils.logging_config import setup_logging

args = getResolvedOptions(sys.argv, ["JOB_NAME", "nav_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

nav_date = args["nav_date"]
logger = setup_logging("nav_calculation", job_name=args["JOB_NAME"])

# ─── READ INPUTS ───
positions_df = spark.read.parquet(f"s3://acme-data-lake/silver/risk_metrics/calc_date={nav_date}/")
cash_df = spark.read.parquet(f"s3://acme-data-lake/silver/cash_balances/date={nav_date}/")
accruals_df = spark.read.parquet(f"s3://acme-data-lake/silver/accruals/date={nav_date}/")
shares_df = spark.read.parquet("s3://acme-data-lake/reference/shares_outstanding/latest/")

# ─── CALCULATE NAV ───
nav_df = calculate_fund_nav(positions_df, cash_df, accruals_df, shares_df)
logger.info(f"Calculated NAV for {nav_df.count()} funds")

# ─── RECONCILE WITH PRIOR DAY (sanity check) ───
try:
    prior_nav = spark.read.parquet(f"s3://acme-data-lake/gold/nav/nav_date={nav_date}/")
    recon = ReconciliationEngine("current", "prior_day")
    result = recon.count_reconciliation(nav_df, prior_nav)
    logger.info(f"NAV fund count: today={result['source_count']}, yesterday={result['target_count']}")
except Exception:
    logger.info("No prior NAV found (first run or new month)")

# ─── VALIDATE ───
validator = DataValidator(nav_df, "nav_calculation")
validator.check_not_null(["fund_id", "nav_per_share", "net_asset_value"])
validator.check_range("nav_per_share", min_val=0.01, max_val=100_000.0)
validator.validate(fail_on_error=True)

# ─── WRITE GOLD ───
nav_df.write.mode("overwrite").parquet(f"s3://acme-data-lake/gold/nav/nav_date={nav_date}/")

# Also write to Redshift for reporting
nav_df.write     .format("jdbc")     .option("url", "jdbc:redshift://acme-cluster.xyz.us-east-1.redshift.amazonaws.com:5439/analytics")     .option("dbtable", "gold.daily_nav")     .option("user", "etl_service")     .option("password", "${ssm:/prod/redshift/password}")     .mode("append")     .save()

job.commit()
logger.info(f"✅ NAV calculation complete for {nav_date}")
