"""
file_arrival_trigger — Lambda triggered by S3 PutObject event.
Determines which pipeline to run based on file path, fetches config from DynamoDB,
starts the appropriate Step Functions state machine.

Trigger: S3 Event Notification (s3:ObjectCreated:*)
Bucket: acme-inbound-files
Prefixes: vendor/, internal/, partner/
"""
import json
import boto3
import os
from datetime import datetime

sfn_client = boto3.client("stepfunctions")
dynamodb = boto3.resource("dynamodb")
config_table = dynamodb.Table(os.environ.get("CONFIG_TABLE", "pipeline_config"))

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def handler(event, context):
    """Process S3 event and trigger appropriate pipeline."""
    
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        size = record["s3"]["object"]["size"]
        event_time = record["eventTime"]
        
        print(f"File arrived: s3://{bucket}/{key} ({size} bytes)")
        
        # Determine pipeline from file path
        # Format: {source_type}/{vendor_name}/{date}/{filename}
        parts = key.split("/")
        if len(parts) < 3:
            print(f"Skipping unexpected path format: {key}")
            continue
        
        source_type = parts[0]  # vendor, internal, partner
        source_name = parts[1]  # bloomberg, refinitiv, payroll, etc.
        
        # Fetch pipeline config from DynamoDB
        config = get_pipeline_config(source_type, source_name)
        if not config:
            print(f"No config found for {source_type}/{source_name}, skipping")
            continue
        
        # Start Step Functions execution
        execution_input = {
            "bucket": bucket,
            "key": key,
            "size": size,
            "source_type": source_type,
            "source_name": source_name,
            "event_time": event_time,
            "pipeline_config": config,
            "execution_id": f"{source_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        }
        
        response = sfn_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_input["execution_id"],
            input=json.dumps(execution_input, default=str),
        )
        
        print(f"Started execution: {response['executionArn']}")
    
    return {"statusCode": 200, "body": f"Processed {len(event['Records'])} events"}


def get_pipeline_config(source_type, source_name):
    """Fetch pipeline configuration from DynamoDB."""
    try:
        response = config_table.get_item(
            Key={"source_type": source_type, "source_name": source_name}
        )
        return response.get("Item")
    except Exception as e:
        print(f"Error fetching config: {e}")
        return None
