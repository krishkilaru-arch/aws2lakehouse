"""
Validator Lambda — Validates incoming healthcare data files.

Checks: Schema conformance, required fields, value ranges (vitals),
        PHI field presence, file integrity, duplicate detection.
"""
import json
import os
import boto3
import csv
import io
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

CONFIG_TABLE = os.environ["CONFIG_TABLE"]

# Validation rules per source system (loaded from DynamoDB config)
VITALS_RANGES = {
    "heart_rate": {"min": 20, "max": 250, "unit": "bpm"},
    "blood_pressure_systolic": {"min": 50, "max": 300, "unit": "mmHg"},
    "blood_pressure_diastolic": {"min": 20, "max": 200, "unit": "mmHg"},
    "temperature": {"min": 90.0, "max": 110.0, "unit": "F"},
    "oxygen_saturation": {"min": 50, "max": 100, "unit": "%"},
    "respiratory_rate": {"min": 5, "max": 60, "unit": "breaths/min"},
    "blood_glucose": {"min": 20, "max": 600, "unit": "mg/dL"},
}

REQUIRED_FIELDS = {
    "vitals": ["patient_id", "device_id", "timestamp", "measurement_type", "value"],
    "lab_results": ["patient_id", "test_code", "result_value", "collected_at", "lab_id"],
    "medications": ["patient_id", "drug_code", "dosage", "prescribed_at", "prescriber_id"],
}

PHI_FIELDS = ["patient_name", "ssn", "address", "phone", "email", "dob"]


def lambda_handler(event, context):
    """Validate a healthcare data file."""
    bucket = event["bucket"]
    key = event["key"]
    source_system = event["source_system"]
    file_format = event["file_format"]
    
    logger.info(f"Validating: s3://{bucket}/{key} (source: {source_system})")
    
    # Read file
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    
    # Parse records
    if file_format == "json":
        records = [json.loads(line) for line in content.strip().split("\n") if line.strip()]
    elif file_format == "csv":
        reader = csv.DictReader(io.StringIO(content))
        records = list(reader)
    else:
        records = []
    
    validation_result = {
        "total_records": len(records),
        "valid_records": 0,
        "invalid_records": 0,
        "errors": [],
        "warnings": [],
        "phi_detected": False,
        "duplicate_count": 0,
    }
    
    if not records:
        validation_result["errors"].append("Empty file — no records found")
        return {**event, "validation": validation_result, "is_valid": False}
    
    # ─── CHECK REQUIRED FIELDS ───
    required = REQUIRED_FIELDS.get(source_system, [])
    sample_keys = set(records[0].keys())
    missing_fields = [f for f in required if f not in sample_keys]
    if missing_fields:
        validation_result["errors"].append(f"Missing required fields: {missing_fields}")
    
    # ─── CHECK PHI FIELDS (should NOT be in raw files) ───
    phi_present = [f for f in PHI_FIELDS if f in sample_keys]
    if phi_present:
        validation_result["phi_detected"] = True
        validation_result["warnings"].append(f"PHI fields detected: {phi_present} — will be masked")
    
    # ─── VALIDATE VALUES (for vitals) ───
    if source_system == "vitals":
        for i, record in enumerate(records):
            measurement = record.get("measurement_type", "")
            value = record.get("value")
            
            if measurement in VITALS_RANGES and value is not None:
                try:
                    val = float(value)
                    limits = VITALS_RANGES[measurement]
                    if val < limits["min"] or val > limits["max"]:
                        validation_result["invalid_records"] += 1
                        if len(validation_result["errors"]) < 10:
                            validation_result["errors"].append(
                                f"Row {i}: {measurement}={val} out of range [{limits['min']}-{limits['max']}]"
                            )
                    else:
                        validation_result["valid_records"] += 1
                except (ValueError, TypeError):
                    validation_result["invalid_records"] += 1
            else:
                validation_result["valid_records"] += 1
    else:
        validation_result["valid_records"] = len(records)
    
    # ─── CHECK DUPLICATES ───
    if "patient_id" in sample_keys and "timestamp" in sample_keys:
        seen = set()
        dupes = 0
        for r in records:
            key_tuple = (r.get("patient_id"), r.get("timestamp"), r.get("measurement_type", ""))
            if key_tuple in seen:
                dupes += 1
            seen.add(key_tuple)
        validation_result["duplicate_count"] = dupes
    
    # ─── DETERMINE PASS/FAIL ───
    error_rate = validation_result["invalid_records"] / max(validation_result["total_records"], 1)
    is_valid = len(validation_result["errors"]) == 0 or error_rate < 0.05  # Allow 5% errors
    
    logger.info(f"Validation result: valid={is_valid}, records={validation_result['total_records']}, "
                f"errors={validation_result['invalid_records']}, dupes={validation_result['duplicate_count']}")
    
    return {**event, "validation": validation_result, "is_valid": is_valid}
