"""
Bloomberg Connector — Databricks Edition.

Changes from AWS version:
  - S3 paths → Unity Catalog Volumes
  - boto3 → dbutils.secrets
  - Glue DynamicFrame → native Spark DataFrame
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class BloombergConnector:
    """Connect to Bloomberg data via Unity Catalog Volumes."""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.volume_path = config.get("volume_path", "/Volumes/production/raw/bloomberg")
    
    def read_daily_pricing(self, trade_date: str) -> DataFrame:
        """Read daily pricing from Volume (Auto Loader compatible)."""
        path = f"{self.volume_path}/pricing/{trade_date}/"
        logger.info(f"Reading Bloomberg pricing from Volume: {path}")
        return (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )
    
    def read_daily_pricing_stream(self, trade_date: str = None) -> DataFrame:
        """Auto Loader: Stream new pricing files as they arrive."""
        return (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", "/Volumes/production/raw/_schemas/bloomberg_pricing")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{self.volume_path}/pricing/")
        )
    
    def read_corporate_actions(self, start_date: str, end_date: str) -> DataFrame:
        """Read corporate actions from Delta table."""
        return (
            self.spark.table("production.reference.corporate_actions")
            .filter(f"effective_date BETWEEN '{start_date}' AND '{end_date}'")
        )
    
    def read_reference_data(self) -> DataFrame:
        """Read instrument reference from Delta table."""
        return self.spark.table("production.reference.instruments")
