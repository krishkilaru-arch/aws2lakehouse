"""
PostgreSQL Extractor — Full and incremental extraction patterns.

Sources: orders_db, customers_db, products_db (all PostgreSQL on RDS)
"""
import boto3
import json
import psycopg2
from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class PostgresExtractor:
    """Extract data from PostgreSQL with full/incremental support."""
    
    def __init__(self, spark: SparkSession, connection_name: str):
        self.spark = spark
        self.connection_name = connection_name
        self._creds = self._get_credentials()
    
    def _get_credentials(self) -> dict:
        """Fetch from AWS Secrets Manager."""
        client = boto3.client("secretsmanager", region_name="us-east-1")
        secret = client.get_secret_value(SecretId=f"prod/rds/{self.connection_name}")
        return json.loads(secret["SecretString"])
    
    @property
    def jdbc_url(self) -> str:
        c = self._creds
        return f"jdbc:postgresql://{c['host']}:{c['port']}/{c['database']}"
    
    def extract_full(self, table: str, partition_column: str = None,
                    num_partitions: int = 10) -> DataFrame:
        """Full table extraction with optional parallel reads."""
        reader = (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("user", self._creds["username"])
            .option("password", self._creds["password"])
            .option("dbtable", table)
            .option("fetchsize", "50000")
            .option("driver", "org.postgresql.Driver")
        )
        
        if partition_column:
            bounds = self._get_bounds(table, partition_column)
            reader = (reader
                .option("partitionColumn", partition_column)
                .option("lowerBound", bounds[0])
                .option("upperBound", bounds[1])
                .option("numPartitions", num_partitions))
        
        return reader.load()
    
    def extract_incremental(self, table: str, watermark_column: str,
                           last_watermark: str) -> DataFrame:
        """CDC-style incremental extraction using watermark."""
        query = f"""
            (SELECT * FROM {table} 
             WHERE {watermark_column} > '{last_watermark}'
             ORDER BY {watermark_column}) AS incremental
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("user", self._creds["username"])
            .option("password", self._creds["password"])
            .option("dbtable", query)
            .option("fetchsize", "50000")
            .load()
        )
    
    def _get_bounds(self, table, column):
        conn = psycopg2.connect(**{k: self._creds[k] for k in ["host", "port", "database", "username", "password"]})
        cur = conn.cursor()
        cur.execute(f"SELECT MIN({column}), MAX({column}) FROM {table}")
        result = cur.fetchone()
        conn.close()
        return result
