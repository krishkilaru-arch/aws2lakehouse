"""
Audit trail utilities — adds metadata columns and logs pipeline execution.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import boto3
import json
from datetime import datetime


def add_audit_columns(df: DataFrame, pipeline_name: str, 
                      batch_id: str = None) -> DataFrame:
    """Add standard audit columns to DataFrame."""
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_pipeline_name", F.lit(pipeline_name))
        .withColumn("_batch_id", F.lit(batch_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")))
        .withColumn("_source_file", F.input_file_name())
    )


def audit_log(pipeline_name: str, status: str, row_count: int,
              duration_seconds: float, details: dict = None):
    """Log pipeline execution to DynamoDB audit table."""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("etl_audit_log")
    
    table.put_item(Item={
        "pipeline_name": pipeline_name,
        "execution_time": datetime.utcnow().isoformat(),
        "status": status,
        "row_count": row_count,
        "duration_seconds": int(duration_seconds),
        "details": json.dumps(details or {}),
    })
