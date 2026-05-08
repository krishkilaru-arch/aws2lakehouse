"""conftest.py — Shared fixtures for aws2lakehouse tests."""

import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_dir():
    """Create a temporary directory that is cleaned up after the test."""
    d = tempfile.mkdtemp(prefix="a2l_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_aws_project(tmp_dir):
    """Create a minimal AWS project structure for scanning tests."""
    project = os.path.join(tmp_dir, "aws-project")

    # Airflow DAG
    dag_dir = os.path.join(project, "airflow", "dags")
    os.makedirs(dag_dir)
    Path(os.path.join(dag_dir, "daily_etl.py")).write_text('''
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {"owner": "data-eng", "retries": 2}

with DAG(
    "daily_customer_etl",
    schedule_interval="0 6 * * *",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    tags=["customer", "pii"],
) as dag:
    task1 = PythonOperator(task_id="extract", python_callable=lambda: None)
    task2 = PythonOperator(task_id="transform", python_callable=lambda: None)
    task1 >> task2
''')

    # EMR script
    emr_dir = os.path.join(project, "emr_jobs")
    os.makedirs(emr_dir)
    Path(os.path.join(emr_dir, "trade_events_streaming.sh")).write_text('''#!/bin/bash
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --num-executors 8 \\
    --topic trade-events \\
    --checkpoint s3://bucket/checkpoints/trades \\
    s3://bucket/scripts/trade_processor.py
''')

    # Glue job
    glue_dir = os.path.join(project, "glue_jobs")
    os.makedirs(glue_dir)
    Path(os.path.join(glue_dir, "payment_processor.py")).write_text('''
import sys
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read from JDBC
df = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={"url": "jdbc:postgresql://host/db", "dbtable": "payments"},
)
# Mask account numbers
df_masked = df.toDF().withColumn("account_number", lit("****"))
df_masked.write.format("parquet").save("s3://bucket/output/payments/")
''')

    # Step Function
    sf_dir = os.path.join(project, "step_functions")
    os.makedirs(sf_dir)
    Path(os.path.join(sf_dir, "etl_pipeline.json")).write_text(json.dumps({
        "Comment": "ETL Pipeline",
        "StartAt": "ExtractData",
        "States": {
            "ExtractData": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {"JobName": "extract_job"},
                "Next": "TransformData"
            },
            "TransformData": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {"JobName": "transform_job"},
                "End": True
            }
        }
    }, indent=2))

    # Config
    config_dir = os.path.join(project, "config")
    os.makedirs(config_dir)
    Path(os.path.join(config_dir, "settings.json")).write_text(json.dumps({
        "environment": "production",
        "region": "us-east-1",
    }))

    return project


@pytest.fixture
def sample_inventory():
    """Return a minimal pipeline inventory list."""
    return [
        {
            "name": "daily_customer_etl",
            "domain": "customer",
            "source_type": "auto_loader",
            "source_config": {},
            "schedule": "0 6 * * *",
            "sla_minutes": 60,
            "business_impact": "high",
            "owner": "data-eng@company.com",
            "mnpi_columns": [],
            "pii_columns": ["account_number"],
            "classification": "confidential",
            "num_executors": 10,
            "tables_written": [],
            "tables_read": [],
        },
        {
            "name": "trade_events_streaming",
            "domain": "risk",
            "source_type": "kafka",
            "source_config": {"topic": "trade-events"},
            "schedule": "continuous",
            "sla_minutes": 5,
            "business_impact": "critical",
            "owner": "data-eng@company.com",
            "mnpi_columns": ["trade_price", "volume"],
            "pii_columns": [],
            "classification": "mnpi",
            "num_executors": 8,
            "tables_written": [],
            "tables_read": [],
        },
        {
            "name": "payment_processor",
            "domain": "finance",
            "source_type": "jdbc",
            "source_config": {},
            "schedule": "*/15 * * * *",
            "sla_minutes": 10,
            "business_impact": "high",
            "owner": "data-eng@company.com",
            "mnpi_columns": [],
            "pii_columns": ["account_number"],
            "classification": "confidential",
            "tables_written": [],
            "tables_read": [],
        },
    ]
