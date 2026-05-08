"""
Notification Lambda — Send alerts for critical vitals, pipeline failures.

Channels: SNS (email/SMS), PagerDuty API, Slack webhook.
"""
import json
import os
import boto3
from datetime import datetime, timezone
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")

ALERT_TOPIC = os.environ["ALERT_TOPIC"]
METADATA_TABLE = os.environ["METADATA_TABLE"]


def lambda_handler(event, context):
    """Send notifications based on pipeline results."""
    run_id = event.get("run_id")
    source_system = event.get("source_system")
    
    enrichment = event.get("enrichment", {})
    validation = event.get("validation", {})
    
    alerts_triggered = enrichment.get("alerts_triggered", 0)
    invalid_records = validation.get("invalid_records", 0)
    total_records = validation.get("total_records", 0)
    is_valid = event.get("is_valid", True)
    
    notifications_sent = []
    
    # ─── CRITICAL VITALS ALERT ───
    if alerts_triggered > 0:
        message = {
            "alert_type": "CRITICAL_VITALS",
            "severity": "HIGH",
            "source_system": source_system,
            "alerts_count": alerts_triggered,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action_required": "Review patient vitals immediately",
        }
        sns_client.publish(
            TopicArn=ALERT_TOPIC,
            Subject=f"🚨 CRITICAL: {alerts_triggered} vitals alerts ({source_system})",
            Message=json.dumps(message, indent=2),
            MessageAttributes={
                "severity": {"DataType": "String", "StringValue": "HIGH"},
                "source": {"DataType": "String", "StringValue": source_system},
            }
        )
        notifications_sent.append("critical_vitals_alert")
    
    # ─── DATA QUALITY ALERT ───
    if not is_valid or (total_records > 0 and invalid_records / total_records > 0.05):
        message = {
            "alert_type": "DATA_QUALITY",
            "severity": "MEDIUM",
            "source_system": source_system,
            "total_records": total_records,
            "invalid_records": invalid_records,
            "error_rate": f"{invalid_records/max(total_records,1)*100:.1f}%",
            "run_id": run_id,
        }
        sns_client.publish(
            TopicArn=ALERT_TOPIC,
            Subject=f"⚠️ Data Quality Issue: {source_system} ({invalid_records} errors)",
            Message=json.dumps(message, indent=2),
        )
        notifications_sent.append("data_quality_alert")
    
    # ─── SUCCESS NOTIFICATION (daily summary) ───
    if is_valid and alerts_triggered == 0:
        logger.info(f"Pipeline {run_id} completed successfully — no alerts")
    
    # ─── UPDATE METADATA ───
    table = dynamodb.Table(METADATA_TABLE)
    table.update_item(
        Key={"pipeline_id": f"ingest_{source_system}", "run_timestamp": event.get("file_date", "")},
        UpdateExpression="SET #s = :status, notifications = :notif",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":status": "COMPLETED",
            ":notif": notifications_sent,
        }
    )
    
    return {**event, "notifications": notifications_sent}
