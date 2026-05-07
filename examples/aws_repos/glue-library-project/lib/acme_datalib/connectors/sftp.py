"""
SFTP Connector — process vendor file drops (custodians, administrators, prime brokers).

Pattern: Vendor drops files to SFTP → AWS Transfer Family → S3 → Glue job picks up
"""
import boto3
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class SFTPFileProcessor:
    """Process files landed from SFTP transfers into S3."""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.landing_bucket = config["landing_bucket"]
        self.archive_bucket = config.get("archive_bucket", config["landing_bucket"])
        self.s3_client = boto3.client("s3")
    
    def list_new_files(self, prefix: str, extension: str = ".csv") -> List[str]:
        """List unprocessed files in the landing zone."""
        paginator = self.s3_client.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.landing_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(extension) and "/processed/" not in obj["Key"]:
                    files.append(f"s3://{self.landing_bucket}/{obj['Key']}")
        return files
    
    def read_vendor_file(self, path: str, schema: Optional[dict] = None,
                        delimiter: str = ",", header: bool = True) -> DataFrame:
        """Read a single vendor file with configurable parsing."""
        reader = (
            self.spark.read
            .option("header", str(header).lower())
            .option("delimiter", delimiter)
            .option("multiLine", "true")
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
        )
        
        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")
        
        return reader.csv(path)
    
    def archive_file(self, source_key: str) -> None:
        """Move processed file to archive location."""
        archive_key = source_key.replace("/landing/", "/archive/")
        self.s3_client.copy_object(
            Bucket=self.archive_bucket,
            CopySource={"Bucket": self.landing_bucket, "Key": source_key},
            Key=archive_key
        )
        self.s3_client.delete_object(Bucket=self.landing_bucket, Key=source_key)
        logger.info(f"Archived: {source_key} → {archive_key}")
