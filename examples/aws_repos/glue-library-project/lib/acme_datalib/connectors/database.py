"""
Database Connector — connects to PostgreSQL, Oracle, SQL Server.

Handles: OMS (Order Management System), PMS (Portfolio Management System),
         Custodian feeds, Compliance systems.
"""
import boto3
import json
from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """Generic JDBC connector with AWS Secrets Manager integration."""
    
    def __init__(self, spark: SparkSession, secret_name: str, region: str = "us-east-1"):
        self.spark = spark
        self.secret_name = secret_name
        self.region = region
        self._credentials = None
    
    @property
    def credentials(self) -> Dict[str, str]:
        """Fetch credentials from AWS Secrets Manager (cached)."""
        if self._credentials is None:
            client = boto3.client("secretsmanager", region_name=self.region)
            response = client.get_secret_value(SecretId=self.secret_name)
            self._credentials = json.loads(response["SecretString"])
        return self._credentials
    
    @property
    def jdbc_url(self) -> str:
        creds = self.credentials
        engine = creds.get("engine", "postgresql")
        host = creds["host"]
        port = creds["port"]
        dbname = creds["dbname"]
        
        if engine == "postgresql":
            return f"jdbc:postgresql://{host}:{port}/{dbname}"
        elif engine == "oracle":
            return f"jdbc:oracle:thin:@{host}:{port}:{dbname}"
        elif engine == "sqlserver":
            return f"jdbc:sqlserver://{host}:{port};databaseName={dbname}"
        else:
            raise ValueError(f"Unsupported engine: {engine}")
    
    def read_table(self, table: str, partition_column: Optional[str] = None,
                   num_partitions: int = 10, predicates: Optional[list] = None) -> DataFrame:
        """Read a table with optional parallel partitioned reads."""
        creds = self.credentials
        
        reader = (
            self.spark.read
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("user", creds["username"])
            .option("password", creds["password"])
            .option("dbtable", table)
            .option("fetchsize", "10000")
        )
        
        if partition_column:
            # Get bounds for partitioning
            bounds_df = self.spark.read.format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("user", creds["username"]) \
                .option("password", creds["password"]) \
                .option("dbtable", f"(SELECT MIN({partition_column}) as mn, MAX({partition_column}) as mx FROM {table}) t") \
                .load()
            
            bounds = bounds_df.collect()[0]
            reader = (reader
                .option("partitionColumn", partition_column)
                .option("lowerBound", str(bounds["mn"]))
                .option("upperBound", str(bounds["mx"]))
                .option("numPartitions", str(num_partitions)))
        
        return reader.load()
    
    def read_incremental(self, table: str, watermark_column: str, 
                         last_watermark: str) -> DataFrame:
        """Read incrementally using a watermark (high-water mark pattern)."""
        query = f"(SELECT * FROM {table} WHERE {watermark_column} > '{last_watermark}') incremental"
        return self.read_table(query)
