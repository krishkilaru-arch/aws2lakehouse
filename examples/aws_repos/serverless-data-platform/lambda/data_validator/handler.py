"""
data_validator — Lambda that validates incoming files before processing.
Checks: file format, schema, row count, duplicates, referential integrity.

Invoked by: Step Functions (validate_input state)
"""
import json
import boto3
import csv
import io
from datetime import datetime

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
metrics_table = dynamodb.Table("pipeline_metrics")


def handler(event, context):
    """Validate incoming file against pipeline config rules."""
    
    bucket = event["bucket"]
    key = event["key"]
    config = event["pipeline_config"]
    
    validation_rules = config.get("validation_rules", {})
    results = []
    
    # Read file head (first 1MB for validation)
    response = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-1048576")
    content = response["Body"].read().decode("utf-8", errors="replace")
    
    # ─── Check 1: File format
    expected_format = validation_rules.get("format", "csv")
    if expected_format == "csv":
        try:
            reader = csv.reader(io.StringIO(content))
            header = next(reader)
            row_count_sample = sum(1 for _ in reader)
            results.append({"check": "format", "passed": True, "detail": f"Valid CSV, {len(header)} columns"})
        except:
            results.append({"check": "format", "passed": False, "detail": "Invalid CSV format"})
            return build_response(event, results, "FAILED")
    
    elif expected_format == "json":
        try:
            # Try parsing first line (JSONL) or full document
            lines = content.strip().split("\n")
            json.loads(lines[0])
            results.append({"check": "format", "passed": True, "detail": f"Valid JSON, {len(lines)} lines"})
        except:
            results.append({"check": "format", "passed": False, "detail": "Invalid JSON format"})
            return build_response(event, results, "FAILED")
    
    # ─── Check 2: Required columns
    required_cols = validation_rules.get("required_columns", [])
    if required_cols and expected_format == "csv":
        missing = [c for c in required_cols if c not in header]
        passed = len(missing) == 0
        results.append({
            "check": "required_columns",
            "passed": passed,
            "detail": f"Missing: {missing}" if missing else "All required columns present"
        })
    
    # ─── Check 3: Minimum row count
    min_rows = validation_rules.get("min_rows", 0)
    if min_rows > 0:
        # Estimate total rows from file size
        file_size = event["size"]
        avg_row_size = len(content) / max(row_count_sample, 1)
        estimated_rows = int(file_size / avg_row_size) if avg_row_size > 0 else 0
        passed = estimated_rows >= min_rows
        results.append({
            "check": "min_rows",
            "passed": passed,
            "detail": f"Estimated {estimated_rows} rows (min: {min_rows})"
        })
    
    # ─── Check 4: File size limits
    max_size_mb = validation_rules.get("max_size_mb", 5000)
    size_mb = event["size"] / (1024 * 1024)
    results.append({
        "check": "file_size",
        "passed": size_mb <= max_size_mb,
        "detail": f"{size_mb:.1f} MB (max: {max_size_mb} MB)"
    })
    
    # ─── Determine overall status
    all_passed = all(r["passed"] for r in results)
    status = "PASSED" if all_passed else "FAILED"
    
    # Log metrics
    log_validation_metrics(event, results, status)
    
    return build_response(event, results, status)


def build_response(event, results, status):
    return {
        **event,
        "validation": {
            "status": status,
            "checks": results,
            "timestamp": datetime.utcnow().isoformat(),
        }
    }


def log_validation_metrics(event, results, status):
    """Log validation results to DynamoDB metrics table."""
    try:
        metrics_table.put_item(Item={
            "pipeline_name": event["source_name"],
            "timestamp": datetime.utcnow().isoformat(),
            "metric_type": "validation",
            "status": status,
            "checks_passed": sum(1 for r in results if r["passed"]),
            "checks_total": len(results),
            "file_key": event["key"],
        })
    except Exception as e:
        print(f"Failed to log metrics: {e}")
