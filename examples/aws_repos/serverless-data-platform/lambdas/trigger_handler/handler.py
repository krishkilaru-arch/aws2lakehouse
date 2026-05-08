"""
Trigger Handler Lambda — Receives S3 EventBridge events, starts Step Function.

Triggered by: EventBridge rule when files land in s3://medstream-landing/incoming/
Action: Validates file metadata, records in DynamoDB, starts ingestion state machine
"""
import json
import os
import boto3
from datetime import datetime, timezone
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sfn_client = boto3.client("stepfunctions")
dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

METADATA_TABLE = os.environ["METADATA_TABLE"]
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")


def lambda_handler(event, context):
    """Handle S3 Object Created events from EventBridge."""
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Parse S3 event
    detail = event.get("detail", {})
    bucket = detail.get("bucket", {}).get("name")
    key = detail.get("object", {}).get("key")
    size = detail.get("object", {}).get("size", 0)
    
    if not bucket or not key:
        logger.error("Missing bucket/key in event")
        return {"statusCode": 400, "body": "Invalid event"}
    
    # Extract metadata from key pattern: incoming/{source}/{date}/{filename}
    parts = key.split("/")
    if len(parts) < 4:
        logger.warning(f"Unexpected key format: {key}")
        return {"statusCode": 400, "body": "Invalid key format"}
    
    source_system = parts[1]  # e.g., "vitals", "lab_results", "medications"
    file_date = parts[2]
    filename = parts[3]
    
    # Determine file format
    if filename.endswith(".json") or filename.endswith(".jsonl"):
        file_format = "json"
    elif filename.endswith(".csv"):
        file_format = "csv"
    elif filename.endswith(".hl7") or filename.endswith(".fhir"):
        file_format = "hl7_fhir"
    else:
        file_format = "unknown"
    
    # Record in metadata table
    run_id = f"{source_system}_{file_date}_{context.aws_request_id[:8]}"
    table = dynamodb.Table(METADATA_TABLE)
    table.put_item(Item={
        "pipeline_id": f"ingest_{source_system}",
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "status": "TRIGGERED",
        "source_bucket": bucket,
        "source_key": key,
        "file_size_bytes": size,
        "file_format": file_format,
        "source_system": source_system,
        "file_date": file_date,
        "ttl": int(datetime.now(timezone.utc).timestamp()) + (90 * 86400),  # 90 days
    })
    
    # Start Step Function
    input_payload = {
        "run_id": run_id,
        "bucket": bucket,
        "key": key,
        "source_system": source_system,
        "file_date": file_date,
        "file_format": file_format,
        "file_size_bytes": size,
    }
    
    response = sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=run_id,
        input=json.dumps(input_payload),
    )
    
    logger.info(f"Started execution: {response['executionArn']}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "run_id": run_id,
            "execution_arn": response["executionArn"],
        })
    }
