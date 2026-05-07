"""
market_data_ingest_job.py — Glue Job: Ingest Bloomberg daily pricing into Bronze.

Schedule: Daily at 7:00 AM ET (after Bloomberg file drop at 6:30 AM)
Source: s3://acme-vendor-drops/bloomberg/pricing/{date}/
Target: s3://acme-data-lake/bronze/market_data/
Library: acme_datalib (--extra-py-files s3://acme-artifacts/libs/acme_datalib-2.1.0-py3-none-any.whl)

Glue Job Parameters:
  --trade_date: YYYY-MM-DD (default: yesterday)
  --environment: prod/staging (default: prod)
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Import shared library
from acme_datalib.connectors.bloomberg import BloombergConnector
from acme_datalib.transformations.market_data import normalize_bloomberg_feed, detect_stale_prices
from acme_datalib.quality.validators import DataValidator
from acme_datalib.utils.logging_config import setup_logging
from acme_datalib.utils.config import PipelineConfig

# Initialize
args = getResolvedOptions(sys.argv, ["JOB_NAME", "trade_date", "environment"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

trade_date = args.get("trade_date", (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
environment = args.get("environment", "prod")
logger = setup_logging("market_data_ingest", job_name=args["JOB_NAME"])

# Load config
config = PipelineConfig.from_s3(
    bucket="acme-config",
    key=f"pipelines/market_data/config.yaml",
    environment=environment
)

# ─── EXTRACT ───
logger.info(f"Extracting Bloomberg pricing for {trade_date}")
bloomberg = BloombergConnector(spark, {
    "s3_bucket": config.get("bloomberg_bucket", "acme-vendor-drops"),
    "s3_prefix": "bloomberg/",
})
raw_df = bloomberg.read_daily_pricing(trade_date)
logger.info(f"Extracted {raw_df.count()} raw records")

# ─── TRANSFORM ───
logger.info("Normalizing Bloomberg feed to standard schema")
normalized_df = normalize_bloomberg_feed(raw_df)

# Add metadata
enriched_df = (
    normalized_df
    .withColumn("trade_date", F.lit(trade_date))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_job_run_id", F.lit(args["JOB_RUN_ID"]))
)

# ─── VALIDATE ───
logger.info("Running data quality checks")
validator = DataValidator(enriched_df, "market_data_ingest")
validator.check_not_null(["instrument_id", "price", "timestamp"])
validator.check_range("price", min_val=0.0, max_val=1_000_000.0)
validator.check_unique(["instrument_id", "price_type", "timestamp"])
validator.validate(fail_on_error=True)

# ─── LOAD ───
output_path = f"s3://acme-data-lake/bronze/market_data/trade_date={trade_date}/"
logger.info(f"Writing to {output_path}")

enriched_df.write     .mode("overwrite")     .partitionBy("source")     .parquet(output_path)

# Update job bookmark
job.commit()
logger.info(f"✅ Market data ingest complete: {enriched_df.count()} records for {trade_date}")
