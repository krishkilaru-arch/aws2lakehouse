"""
Database Connector — Databricks Edition.

Changes: boto3 Secrets Manager → dbutils.secrets
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """JDBC connector using Databricks secret scopes."""
    
    def __init__(self, spark: SparkSession, secret_scope: str):
        self.spark = spark
        self.secret_scope = secret_scope
    
    @property
    def jdbc_url(self) -> str:
        return dbutils.secrets.get(self.secret_scope, "jdbc_url")
    
    def read_table(self, table: str, partition_column: Optional[str] = None,
                   num_partitions: int = 10) -> DataFrame:
        """Read table with parallel partitioned reads."""
        reader = (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("user", dbutils.secrets.get(self.secret_scope, "username"))
            .option("password", dbutils.secrets.get(self.secret_scope, "password"))
            .option("dbtable", table)
            .option("fetchsize", "50000")
        )
        
        if partition_column:
            reader = (reader
                .option("partitionColumn", partition_column)
                .option("numPartitions", str(num_partitions)))
        
        return reader.load()
    
    def read_incremental(self, table: str, watermark_column: str,
                        last_watermark: str) -> DataFrame:
        """Incremental read using watermark."""
        query = f"(SELECT * FROM {table} WHERE {watermark_column} > '{last_watermark}') t"
        return self.read_table(query)
