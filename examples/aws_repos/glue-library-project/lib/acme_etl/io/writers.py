"""
Target writers — S3, Redshift, Glue Catalog.
"""
from pyspark.sql import DataFrame
import boto3
import json


def write_target(df: DataFrame, target_config: dict):
    """Universal writer — routes to correct writer based on target type."""
    target_type = target_config["type"]
    
    if target_type == "s3":
        write_s3(df, target_config)
    elif target_type == "redshift":
        write_redshift(df, target_config)
    else:
        raise ValueError(f"Unsupported target type: {target_type}")


def write_s3(df: DataFrame, config: dict):
    """Write to S3 as Parquet with optional partitioning."""
    writer = df.write.mode(config.get("mode", "overwrite")).format("parquet")
    
    if "partition_by" in config:
        writer = writer.partitionBy(*config["partition_by"])
    
    if config.get("coalesce"):
        df = df.coalesce(config["coalesce"])
    
    writer.save(config["path"])


def write_redshift(df: DataFrame, config: dict):
    """Write to Redshift via JDBC (using temp S3 staging)."""
    from acme_etl.io.readers import get_secret
    secret = get_secret(config["secret_name"])
    
    (
        df.write.format("jdbc")
        .option("url", secret["url"])
        .option("user", secret["username"])
        .option("password", secret["password"])
        .option("dbtable", config["table"])
        .option("tempdir", config.get("temp_s3", "s3://acme-temp/redshift-staging/"))
        .mode(config.get("mode", "append"))
        .save()
    )
