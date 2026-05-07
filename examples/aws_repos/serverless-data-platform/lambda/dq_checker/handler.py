"""
dq_checker — Lambda that runs data quality checks AFTER Glue transformation.
Queries the output table/files and validates business rules.
Acts as a quality gate between Silver and Gold layers.

Invoked by: Step Functions (quality_gate state)
"""
import json
import boto3
import os
from datetime import datetime

athena = boto3.client("athena")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

ALERT_TOPIC = os.environ.get("ALERT_TOPIC_ARN", "")
DQ_DATABASE = "acme_quality"
RESULTS_BUCKET = "acme-athena-results"


def handler(event, context):
    """Run DQ checks on transformed data."""
    
    source_name = event["source_name"]
    config = event["pipeline_config"]
    dq_rules = config.get("dq_rules", [])
    
    results = []
    
    for rule in dq_rules:
        rule_name = rule["name"]
        sql = rule["sql"]
        threshold = rule.get("threshold", 0)
        severity = rule.get("severity", "warning")
        
        # Run Athena query
        try:
            query_result = run_athena_query(sql)
            violation_count = int(query_result[0][0]) if query_result else 0
            
            passed = violation_count <= threshold
            results.append({
                "rule": rule_name,
                "passed": passed,
                "violations": violation_count,
                "threshold": threshold,
                "severity": severity,
            })
            
            if not passed and severity == "critical":
                send_alert(source_name, rule_name, violation_count)
                
        except Exception as e:
            results.append({
                "rule": rule_name,
                "passed": False,
                "error": str(e),
                "severity": severity,
            })
    
    # Overall pass/fail
    critical_failures = [r for r in results if not r["passed"] and r.get("severity") == "critical"]
    status = "FAILED" if critical_failures else "PASSED"
    
    return {
        **event,
        "quality_gate": {
            "status": status,
            "results": results,
            "critical_failures": len(critical_failures),
            "timestamp": datetime.utcnow().isoformat(),
        }
    }


def run_athena_query(sql):
    """Execute Athena query and return results."""
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DQ_DATABASE},
        ResultConfiguration={"OutputLocation": f"s3://{RESULTS_BUCKET}/dq_results/"}
    )
    
    query_id = response["QueryExecutionId"]
    
    # Wait for completion (simplified — production would use Step Functions wait)
    import time
    for _ in range(30):
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(2)
    
    if state != "SUCCEEDED":
        raise Exception(f"Athena query failed: {state}")
    
    results = athena.get_query_results(QueryExecutionId=query_id)
    rows = results["ResultSet"]["Rows"][1:]  # Skip header
    return [[col.get("VarCharValue", "") for col in row["Data"]] for row in rows]


def send_alert(pipeline, rule, violations):
    """Send SNS alert for critical DQ failure."""
    if ALERT_TOPIC:
        sns.publish(
            TopicArn=ALERT_TOPIC,
            Subject=f"🔴 DQ CRITICAL: {pipeline} - {rule}",
            Message=f"Pipeline: {pipeline}\nRule: {rule}\nViolations: {violations}\nTime: {datetime.utcnow()}",
        )
