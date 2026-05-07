"""
Enrichment Lambda — Enrich validated data with patient context from DynamoDB.

Adds: patient demographics, care team, device metadata, alert thresholds.
Masks PHI fields before writing to Bronze layer.
"""
import json
import os
import boto3
from datetime import datetime, timezone
import hashlib
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

PATIENT_TABLE = os.environ.get("PATIENT_TABLE", "medstream-patients-prod")
DATA_BUCKET = os.environ["DATA_BUCKET"]


def lambda_handler(event, context):
    """Enrich records with patient context, mask PHI, write to Bronze."""
    bucket = event["bucket"]
    key = event["key"]
    source_system = event["source_system"]
    file_date = event["file_date"]
    run_id = event["run_id"]
    
    logger.info(f"Enriching: s3://{bucket}/{key}")
    
    # Read validated file
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    records = [json.loads(line) for line in content.strip().split("\n") if line.strip()]
    
    # Load patient registry for enrichment
    patient_table = dynamodb.Table(PATIENT_TABLE)
    patient_cache = {}
    
    enriched_records = []
    for record in records:
        patient_id = record.get("patient_id")
        
        # Fetch patient context (with caching)
        if patient_id and patient_id not in patient_cache:
            try:
                resp = patient_table.get_item(Key={"patient_id": patient_id})
                patient_cache[patient_id] = resp.get("Item", {})
            except Exception as e:
                logger.warning(f"Failed to fetch patient {patient_id}: {e}")
                patient_cache[patient_id] = {}
        
        patient_ctx = patient_cache.get(patient_id, {})
        
        # Enrich
        enriched = {
            **record,
            "age_group": patient_ctx.get("age_group"),
            "care_team_id": patient_ctx.get("care_team_id"),
            "facility_id": patient_ctx.get("facility_id"),
            "risk_category": patient_ctx.get("risk_category"),
            "device_type": patient_ctx.get("device_type"),
            "alert_threshold": patient_ctx.get(f"threshold_{record.get('measurement_type', 'default')}"),
            # Mask PHI
            "patient_id_hash": hashlib.sha256(patient_id.encode()).hexdigest() if patient_id else None,
            # Metadata
            "_enriched_at": datetime.now(timezone.utc).isoformat(),
            "_run_id": run_id,
            "_source_file": key,
        }
        
        # Remove raw PHI fields
        for phi_field in ["patient_name", "ssn", "address", "phone", "email"]:
            enriched.pop(phi_field, None)
        
        # Check for critical alerts
        if source_system == "vitals":
            threshold = enriched.get("alert_threshold")
            value = record.get("value")
            if threshold and value:
                try:
                    if float(value) > float(threshold):
                        enriched["_alert_triggered"] = True
                        enriched["_alert_severity"] = "CRITICAL"
                except (ValueError, TypeError):
                    pass
        
        enriched_records.append(enriched)
    
    # Write to Bronze layer in data lake
    output_key = f"bronze/{source_system}/date={file_date}/{run_id}.jsonl"
    output_content = "\n".join(json.dumps(r) for r in enriched_records)
    
    s3_client.put_object(
        Bucket=DATA_BUCKET,
        Key=output_key,
        Body=output_content.encode("utf-8"),
        ContentType="application/jsonl",
    )
    
    # Count alerts
    alerts = [r for r in enriched_records if r.get("_alert_triggered")]
    
    result = {
        **event,
        "enrichment": {
            "records_enriched": len(enriched_records),
            "patients_resolved": len(patient_cache),
            "alerts_triggered": len(alerts),
            "output_bucket": DATA_BUCKET,
            "output_key": output_key,
        }
    }
    
    logger.info(f"Enrichment complete: {len(enriched_records)} records, {len(alerts)} alerts")
    return result
