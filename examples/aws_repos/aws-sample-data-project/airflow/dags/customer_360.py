"""
customer_360.py — Build unified customer profile from multiple sources.
Combines PostgreSQL, MongoDB, and S3 data into a single customer view.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "customer-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=2),
}

dag = DAG(
    "customer_360",
    default_args=default_args,
    description="Build unified customer 360 profile (PII - restricted access)",
    schedule_interval="0 4 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["customer", "daily", "pii", "critical"],
)

# Source 1: PostgreSQL customer records
extract_rds_customers = GlueJobOperator(
    task_id="extract_rds_customers",
    job_name="customer-extract-rds",
    script_args={
        "--source_table": "public.customers",
        "--target_path": "s3://acme-data-lake/raw/customer_360/rds_customers/",
    },
    dag=dag,
)

# Source 2: MongoDB user sessions
extract_mongo_sessions = GlueJobOperator(
    task_id="extract_mongo_sessions",
    job_name="customer-extract-mongo-sessions",
    script_args={
        "--mongodb_uri": "mongodb+srv://prod-cluster.acme.net/analytics",
        "--collection": "user_sessions",
        "--target_path": "s3://acme-data-lake/raw/customer_360/mongo_sessions/",
        "--lookback_hours": "48",
    },
    dag=dag,
)

# Source 3: Payment history
extract_payments = GlueJobOperator(
    task_id="extract_payments",
    job_name="customer-extract-payments",
    script_args={
        "--source_table": "public.payments",
        "--target_path": "s3://acme-data-lake/raw/customer_360/payments/",
    },
    dag=dag,
)

# Merge all sources into unified profile
build_profile = EmrAddStepsOperator(
    task_id="build_customer_profile",
    job_flow_id="j-ANALYTICS01",
    steps=[{
        "Name": "Build Customer 360",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master", "yarn",
                "--deploy-mode", "cluster",
                "--num-executors", "10",
                "--executor-memory", "8g",
                "s3://acme-jars/customer-360-builder-1.2.jar",
                "--date", "{{ ds }}",
                "--output", "s3://acme-data-lake/curated/customer_360/",
            ],
        },
    }],
    aws_conn_id="aws_default",
    dag=dag,
)

[extract_rds_customers, extract_mongo_sessions, extract_payments] >> build_profile
