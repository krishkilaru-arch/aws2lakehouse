"""
Bloomberg Data Connector — connects to Bloomberg B-PIPE / SFTP drops.

Handles: Real-time feed (B-PIPE), daily file drops (per_security_*.csv),
         corporate actions, reference data.
"""
import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class BloombergConnector:
    """Connect to Bloomberg data sources (S3 drops from B-PIPE)."""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.bucket = config["s3_bucket"]
        self.prefix = config.get("s3_prefix", "bloomberg/")
        self.region = config.get("region", "us-east-1")
        self.encryption_key_id = config.get("kms_key_id")
    
    def read_daily_pricing(self, trade_date: str) -> DataFrame:
        """Read daily end-of-day pricing file from Bloomberg SFTP drop."""
        path = f"s3://{self.bucket}/{self.prefix}pricing/{trade_date}/per_security_*.csv.gz"
        logger.info(f"Reading Bloomberg pricing from: {path}")
        
        df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("compression", "gzip")
            .csv(path)
        )
        logger.info(f"Read {df.count()} pricing records for {trade_date}")
        return df
    
    def read_corporate_actions(self, start_date: str, end_date: str) -> DataFrame:
        """Read corporate actions (splits, dividends, mergers)."""
        path = f"s3://{self.bucket}/{self.prefix}corporate_actions/"
        return (
            self.spark.read.parquet(path)
            .filter(
                (F.col("effective_date") >= start_date) &
                (F.col("effective_date") <= end_date))
        )
    
    def read_reference_data(self) -> DataFrame:
        """Read instrument reference data (GICS, identifiers, etc)."""
        path = f"s3://{self.bucket}/{self.prefix}reference/latest/"
        return self.spark.read.parquet(path)


class BloombergBPipeConnector:
    """Connect to Bloomberg B-PIPE real-time streaming (via Kafka bridge)."""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.kafka_brokers = config["kafka_brokers"]
        self.topic = config.get("topic", "bloomberg-bpipe-raw")
        self.consumer_group = config.get("consumer_group", "acme-market-data")
    
    def read_stream(self) -> DataFrame:
        """Read real-time Bloomberg B-PIPE stream via Kafka."""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_brokers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .option("kafka.group.id", self.consumer_group)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .load()
        )
