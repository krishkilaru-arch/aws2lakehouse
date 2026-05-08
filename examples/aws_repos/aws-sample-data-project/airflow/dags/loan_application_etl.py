"""
loan_application_etl.py — Hourly extract from PostgreSQL loan system.
Extracts new/modified loan applications and loads to S3 data lake.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "lending-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "loan_application_etl",
    default_args=default_args,
    description="Extract loan applications from PostgreSQL every hour",
    schedule_interval="0 * * * *",
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["lending", "hourly", "pii"],
)

# Extract new applications (watermark-based)
extract_applications = GlueJobOperator(
    task_id="extract_applications",
    job_name="lending-extract-applications",
    script_args={
        "--source_table": "public.loan_applications",
        "--target_path": "s3://acme-data-lake/raw/loan_applications/",
        "--watermark_column": "modified_at",
        "--connection_name": "lending-postgres-prod",
    },
    dag=dag,
)

# Extract customer data (for enrichment)
extract_customers = GlueJobOperator(
    task_id="extract_customers",
    job_name="lending-extract-customers",
    script_args={
        "--source_table": "public.customers",
        "--target_path": "s3://acme-data-lake/raw/customers/",
        "--watermark_column": "updated_at",
        "--connection_name": "lending-postgres-prod",
    },
    dag=dag,
)

# Transform: join + clean + PII masking
transform_applications = GlueJobOperator(
    task_id="transform_applications",
    job_name="lending-transform-applications",
    script_args={
        "--input_apps": "s3://acme-data-lake/raw/loan_applications/",
        "--input_customers": "s3://acme-data-lake/raw/customers/",
        "--output": "s3://acme-data-lake/curated/loan_applications/",
    },
    dag=dag,
)

# Dependencies
[extract_applications, extract_customers] >> transform_applications
