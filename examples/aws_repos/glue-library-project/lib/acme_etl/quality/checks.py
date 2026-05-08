"""
Data quality framework — runs checks and returns pass/fail with metrics.
Inspired by Great Expectations but lighter-weight for Glue.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dataclasses import dataclass
from typing import List, Dict
import json
import boto3
from datetime import datetime


@dataclass
class QualityResult:
    check_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: str


def run_quality_checks(df: DataFrame, checks: List[Dict]) -> List[QualityResult]:
    """
    Run a list of quality checks against a DataFrame.
    
    checks format:
    [
        {"type": "not_null", "column": "customer_id"},
        {"type": "unique", "columns": ["order_id"]},
        {"type": "range", "column": "amount", "min": 0, "max": 1000000},
        {"type": "freshness", "column": "event_time", "max_hours": 24},
        {"type": "row_count", "min": 1000, "max": 10000000},
        {"type": "regex", "column": "email", "pattern": r"^[^@]+@[^@]+\.[^@]+$"},
    ]
    """
    results = []
    total_rows = df.count()
    
    for check in checks:
        check_type = check["type"]
        
        if check_type == "not_null":
            col = check["column"]
            null_count = df.filter(F.col(col).isNull()).count()
            null_pct = null_count / total_rows if total_rows > 0 else 0
            threshold = check.get("threshold", 0.0)
            results.append(QualityResult(
                check_name=f"not_null_{col}",
                passed=null_pct <= threshold,
                metric_value=null_pct,
                threshold=threshold,
                details=f"{null_count}/{total_rows} nulls ({null_pct:.2%})"
            ))
        
        elif check_type == "unique":
            cols = check["columns"]
            distinct = df.select(*cols).distinct().count()
            dup_count = total_rows - distinct
            results.append(QualityResult(
                check_name=f"unique_{'_'.join(cols)}",
                passed=dup_count == 0,
                metric_value=dup_count,
                threshold=0,
                details=f"{dup_count} duplicates found"
            ))
        
        elif check_type == "range":
            col = check["column"]
            min_val, max_val = check.get("min"), check.get("max")
            out_of_range = df.filter(
                (F.col(col) < min_val) | (F.col(col) > max_val)
            ).count()
            results.append(QualityResult(
                check_name=f"range_{col}",
                passed=out_of_range == 0,
                metric_value=out_of_range,
                threshold=0,
                details=f"{out_of_range} values outside [{min_val}, {max_val}]"
            ))
        
        elif check_type == "freshness":
            col = check["column"]
            max_hours = check["max_hours"]
            max_ts = df.agg(F.max(col)).collect()[0][0]
            if max_ts:
                staleness_hours = (datetime.utcnow() - max_ts).total_seconds() / 3600
                results.append(QualityResult(
                    check_name=f"freshness_{col}",
                    passed=staleness_hours <= max_hours,
                    metric_value=staleness_hours,
                    threshold=max_hours,
                    details=f"Latest record: {staleness_hours:.1f}h ago (max: {max_hours}h)"
                ))
        
        elif check_type == "row_count":
            min_rows = check.get("min", 0)
            max_rows = check.get("max", float("inf"))
            results.append(QualityResult(
                check_name="row_count",
                passed=min_rows <= total_rows <= max_rows,
                metric_value=total_rows,
                threshold=min_rows,
                details=f"{total_rows} rows (expected [{min_rows}, {max_rows}])"
            ))
    
    return results


def validate_schema(df: DataFrame, expected_columns: Dict[str, str]) -> List[QualityResult]:
    """Validate DataFrame schema matches expected columns and types."""
    results = []
    actual_cols = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    
    for col, expected_type in expected_columns.items():
        if col not in actual_cols:
            results.append(QualityResult(
                f"schema_{col}", False, 0, 1, f"Column {col} MISSING"
            ))
        elif actual_cols[col] != expected_type:
            results.append(QualityResult(
                f"schema_{col}", False, 0, 1,
                f"Column {col}: expected {expected_type}, got {actual_cols[col]}"
            ))
        else:
            results.append(QualityResult(
                f"schema_{col}", True, 1, 1, f"Column {col}: {expected_type} ✓"
            ))
    
    return results


def publish_quality_metrics(results: List[QualityResult], pipeline_name: str,
                           cloudwatch_namespace: str = "AcmeETL/Quality"):
    """Publish quality metrics to CloudWatch."""
    client = boto3.client("cloudwatch", region_name="us-east-1")
    
    metrics = []
    for r in results:
        metrics.append({
            "MetricName": r.check_name,
            "Dimensions": [{"Name": "Pipeline", "Value": pipeline_name}],
            "Value": 1.0 if r.passed else 0.0,
            "Unit": "None",
        })
    
    # Batch put (max 20 per call)
    for i in range(0, len(metrics), 20):
        client.put_metric_data(Namespace=cloudwatch_namespace, MetricData=metrics[i:i+20])
