"""
Metadata Manager Lambda — Track pipeline runs, update watermarks, manage state.

Used by Step Functions for: start/complete tracking, watermark management,
retry state, lineage recording.
"""
import json
import os
import boto3
from datetime import datetime, timezone
from decimal import Decimal
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
METADATA_TABLE = os.environ["METADATA_TABLE"]
CONFIG_TABLE = os.environ["CONFIG_TABLE"]


def lambda_handler(event, context):
    """Manage pipeline metadata and state."""
    action = event.get("action", "record_run")
    
    if action == "record_run":
        return record_pipeline_run(event)
    elif action == "update_watermark":
        return update_watermark(event)
    elif action == "get_config":
        return get_pipeline_config(event)
    elif action == "check_duplicate":
        return check_duplicate_file(event)
    else:
        return {"error": f"Unknown action: {action}"}


def record_pipeline_run(event):
    """Record a pipeline run start/completion."""
    table = dynamodb.Table(METADATA_TABLE)
    
    item = {
        "pipeline_id": event["pipeline_id"],
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": event.get("run_id", ""),
        "status": event.get("status", "RUNNING"),
        "source_system": event.get("source_system", ""),
        "records_processed": event.get("records_processed", 0),
        "duration_seconds": event.get("duration_seconds", 0),
        "errors": event.get("errors", []),
        "ttl": int(datetime.now(timezone.utc).timestamp()) + (90 * 86400),
    }
    
    table.put_item(Item=json.loads(json.dumps(item), parse_float=Decimal))
    return {"recorded": True, "pipeline_id": event["pipeline_id"]}


def update_watermark(event):
    """Update the high-water mark for incremental processing."""
    table = dynamodb.Table(CONFIG_TABLE)
    
    table.update_item(
        Key={"config_key": f"watermark_{event['pipeline_id']}"},
        UpdateExpression="SET watermark_value = :val, updated_at = :ts",
        ExpressionAttributeValues={
            ":val": event["watermark_value"],
            ":ts": datetime.now(timezone.utc).isoformat(),
        }
    )
    return {"updated": True, "watermark": event["watermark_value"]}


def get_pipeline_config(event):
    """Get pipeline configuration from DynamoDB."""
    table = dynamodb.Table(CONFIG_TABLE)
    response = table.get_item(Key={"config_key": event["config_key"]})
    return response.get("Item", {})


def check_duplicate_file(event):
    """Check if this file has already been processed (idempotency)."""
    table = dynamodb.Table(METADATA_TABLE)
    
    response = table.query(
        KeyConditionExpression="pipeline_id = :pid",
        FilterExpression="source_key = :key AND #s = :status",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":pid": event["pipeline_id"],
            ":key": event["source_key"],
            ":status": "COMPLETED",
        },
        Limit=1,
    )
    
    is_duplicate = len(response.get("Items", [])) > 0
    return {"is_duplicate": is_duplicate, "source_key": event["source_key"]}
