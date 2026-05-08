"""
Source readers — abstract away S3/JDBC/Kafka connection details.
All connection strings come from AWS Secrets Manager.
"""
from pyspark.sql import DataFrame, SparkSession
import boto3
import json


def get_secret(secret_name: str, region: str = "us-east-1") -> dict:
    """Fetch credentials from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def read_source(spark: SparkSession, source_config: dict) -> DataFrame:
    """Universal reader — routes to correct reader based on source type."""
    source_type = source_config["type"]
    
    if source_type == "s3":
        return read_s3(spark, source_config)
    elif source_type == "jdbc":
        return read_jdbc(spark, source_config)
    elif source_type == "kafka":
        return read_kafka(spark, source_config)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


def read_s3(spark: SparkSession, config: dict) -> DataFrame:
    """Read from S3 (CSV, JSON, Parquet, Delta)."""
    path = config["path"]
    fmt = config.get("format", "parquet")
    options = config.get("options", {})
    
    reader = spark.read.format(fmt)
    for k, v in options.items():
        reader = reader.option(k, v)
    
    if fmt == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    
    return reader.load(path)


def read_jdbc(spark: SparkSession, config: dict) -> DataFrame:
    """Read from JDBC source (PostgreSQL, MySQL, Oracle)."""
    secret = get_secret(config["secret_name"])
    
    return (
        spark.read.format("jdbc")
        .option("url", secret["url"])
        .option("user", secret["username"])
        .option("password", secret["password"])
        .option("dbtable", config["table"])
        .option("fetchsize", config.get("fetchsize", "10000"))
        .option("partitionColumn", config.get("partition_column", "id"))
        .option("lowerBound", config.get("lower_bound", "0"))
        .option("upperBound", config.get("upper_bound", "10000000"))
        .option("numPartitions", config.get("num_partitions", "20"))
        .load()
    )


def read_kafka(spark: SparkSession, config: dict) -> DataFrame:
    """Read from Kafka (batch or streaming)."""
    secret = get_secret(config["secret_name"])
    
    reader = spark.readStream if config.get("streaming", False) else spark.read
    
    return (
        reader.format("kafka")
        .option("kafka.bootstrap.servers", secret["bootstrap_servers"])
        .option("subscribe", config["topic"])
        .option("startingOffsets", config.get("starting_offsets", "earliest"))
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .load()
    )
