"""
risk_aggregation_job.py — Glue Job: Calculate risk metrics from positions + market data.

Schedule: Daily at 9:00 AM ET (after position snapshot)
Source: s3://acme-data-lake/bronze/positions/, s3://acme-data-lake/bronze/market_data/
Target: s3://acme-data-lake/silver/risk_metrics/
Library: acme_datalib
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

from acme_datalib.transformations.positions import (
    calculate_pnl, aggregate_by_sector, calculate_concentration_risk
)
from acme_datalib.transformations.market_data import detect_stale_prices
from acme_datalib.quality.validators import DataValidator
from acme_datalib.utils.logging_config import setup_logging

args = getResolvedOptions(sys.argv, ["JOB_NAME", "calc_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

calc_date = args["calc_date"]
logger = setup_logging("risk_aggregation", job_name=args["JOB_NAME"])

# ─── READ BRONZE ───
positions_df = spark.read.parquet(f"s3://acme-data-lake/bronze/positions/position_date={calc_date}/")
market_data_df = spark.read.parquet(f"s3://acme-data-lake/bronze/market_data/trade_date={calc_date}/")
reference_df = spark.read.parquet("s3://acme-data-lake/reference/instruments/latest/")

logger.info(f"Read {positions_df.count()} positions, {market_data_df.count()} prices")

# ─── CHECK FOR STALE PRICES ───
stale_check = detect_stale_prices(market_data_df, staleness_threshold_minutes=120)
stale_count = stale_check.filter(F.col("is_stale")).count()
if stale_count > 50:
    logger.error(f"🔴 {stale_count} stale prices detected — risk calc may be inaccurate")

# ─── CALCULATE P&L ───
pnl_df = calculate_pnl(positions_df, market_data_df)
logger.info(f"Calculated P&L for {pnl_df.count()} positions")

# ─── SECTOR AGGREGATION ───
sector_df = aggregate_by_sector(pnl_df, reference_df)

# ─── CONCENTRATION RISK ───
concentration_df = calculate_concentration_risk(pnl_df, threshold_pct=10.0)
breaches = concentration_df.filter(F.col("concentration_breach")).count()
logger.warning(f"Concentration breaches: {breaches}")

# ─── VALIDATE ───
validator = DataValidator(pnl_df, "risk_aggregation")
validator.check_not_null(["account_id", "instrument_id", "market_value"])
validator.check_range("unrealized_pnl", min_val=-1_000_000_000, max_val=1_000_000_000)
validator.validate(fail_on_error=False)  # Don't fail — flag for review

# ─── WRITE SILVER ───
pnl_df.write.mode("overwrite").partitionBy("account_id")     .parquet(f"s3://acme-data-lake/silver/risk_metrics/calc_date={calc_date}/")

sector_df.write.mode("overwrite")     .parquet(f"s3://acme-data-lake/silver/sector_exposure/calc_date={calc_date}/")

concentration_df.filter(F.col("concentration_breach")).write.mode("overwrite")     .parquet(f"s3://acme-data-lake/silver/concentration_breaches/calc_date={calc_date}/")

job.commit()
logger.info(f"✅ Risk aggregation complete for {calc_date}")
