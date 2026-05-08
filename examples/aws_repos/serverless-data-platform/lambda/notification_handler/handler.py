"""
notification_handler — Sends pipeline status notifications.
Triggered at end of Step Function execution (success or failure).
Sends to: Slack webhook, SNS topic, DynamoDB audit log.
"""
import json
import boto3
import os
import urllib.request
from datetime import datetime

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")
audit_table = dynamodb.Table("pipeline_execution_log")

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")
SNS_TOPIC = os.environ.get("NOTIFICATION_TOPIC_ARN", "")


def handler(event, context):
    """Process pipeline completion and send notifications."""
    
    pipeline_name = event.get("source_name", "unknown")
    status = event.get("final_status", "UNKNOWN")
    execution_id = event.get("execution_id", "")
    duration_seconds = event.get("duration_seconds", 0)
    row_count = event.get("row_count", 0)
    
    # Log to DynamoDB
    log_execution(pipeline_name, status, execution_id, duration_seconds, row_count, event)
    
    # Send Slack notification
    if SLACK_WEBHOOK:
        send_slack(pipeline_name, status, duration_seconds, row_count)
    
    # Send SNS (for PagerDuty/email escalation on failures)
    if status == "FAILED" and SNS_TOPIC:
        sns.publish(
            TopicArn=SNS_TOPIC,
            Subject=f"Pipeline FAILED: {pipeline_name}",
            Message=json.dumps(event, indent=2, default=str),
        )
    
    return {"statusCode": 200, "pipeline": pipeline_name, "status": status}


def log_execution(pipeline_name, status, execution_id, duration, rows, full_event):
    """Log execution details to DynamoDB audit table."""
    audit_table.put_item(Item={
        "pipeline_name": pipeline_name,
        "execution_time": datetime.utcnow().isoformat(),
        "execution_id": execution_id,
        "status": status,
        "duration_seconds": int(duration),
        "row_count": int(rows),
        "validation_status": full_event.get("validation", {}).get("status", "N/A"),
        "quality_status": full_event.get("quality_gate", {}).get("status", "N/A"),
        "ttl": int(datetime.utcnow().timestamp()) + (365 * 24 * 3600),  # 1 year retention
    })


def send_slack(pipeline_name, status, duration, rows):
    """Send Slack webhook notification."""
    emoji = "✅" if status == "SUCCESS" else "🔴" if status == "FAILED" else "⚠️"
    color = "#36a64f" if status == "SUCCESS" else "#ff0000"
    
    payload = {
        "attachments": [{
            "color": color,
            "title": f"{emoji} Pipeline: {pipeline_name}",
            "fields": [
                {"title": "Status", "value": status, "short": True},
                {"title": "Duration", "value": f"{duration}s", "short": True},
                {"title": "Rows", "value": f"{rows:,}", "short": True},
            ],
            "footer": f"Serverless Data Platform | {datetime.utcnow().strftime('%H:%M UTC')}",
        }]
    }
    
    req = urllib.request.Request(
        SLACK_WEBHOOK, 
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req)
