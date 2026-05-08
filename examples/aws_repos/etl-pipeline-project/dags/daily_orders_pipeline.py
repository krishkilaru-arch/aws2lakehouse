"""
daily_orders_pipeline.py — Main e-commerce order processing pipeline.

Runs daily at 2 AM UTC. Extracts orders from PostgreSQL, enriches with
product catalog and customer data, calculates revenue metrics, loads to warehouse.

Dependencies: extract_orders >> enrich >> aggregate >> load_warehouse >> notify
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    "owner": "data-platform",
    "depends_on_past": True,
    "email": ["data-alerts@shopmax.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}

dag = DAG(
    "daily_orders_pipeline",
    default_args=default_args,
    description="Daily order processing: extract → enrich → aggregate → warehouse",
    schedule_interval="0 2 * * *",
    start_date=datetime(2023, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["orders", "daily", "critical", "revenue"],
)

# ─── SENSORS (wait for upstream data) ───
wait_for_orders = S3KeySensor(
    task_id="wait_for_orders_export",
    bucket_name="shopmax-raw-data",
    bucket_key="exports/orders/{{ ds }}/orders_*.jsonl.gz",
    wildcard_match=True,
    timeout=3600,
    poke_interval=120,
    dag=dag,
)

# ─── EXTRACT (multiple sources) ───
extract_orders = BashOperator(
    task_id="extract_orders",
    bash_command="""
        python /opt/airflow/dags/scripts/extract_orders.py             --date {{ ds }}             --output s3://shopmax-staging/orders/{{ ds }}/             --source-db orders_replica
    """,
    dag=dag,
)

extract_products = BashOperator(
    task_id="extract_products",
    bash_command="""
        python /opt/airflow/dags/scripts/extract_products.py             --date {{ ds }}             --output s3://shopmax-staging/products/{{ ds }}/
    """,
    dag=dag,
)

extract_customers = BashOperator(
    task_id="extract_customers",
    bash_command="""
        python /opt/airflow/dags/scripts/extract_customers.py             --date {{ ds }}             --output s3://shopmax-staging/customers/{{ ds }}/             --incremental --watermark-column updated_at
    """,
    dag=dag,
)

# ─── EMR TRANSFORMATION (heavy joins + aggregation) ───
EMR_CONFIG = {
    "Name": "shopmax-daily-etl-{{ ds }}",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hive"}],
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master", "Market": "ON_DEMAND", "InstanceRole": "MASTER",
             "InstanceType": "m5.2xlarge", "InstanceCount": 1},
            {"Name": "Core", "Market": "SPOT", "InstanceRole": "CORE",
             "InstanceType": "r5.4xlarge", "InstanceCount": 8,
             "BidPrice": "OnDemandPrice"},
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://shopmax-emr-logs/",
    "Tags": [{"Key": "team", "Value": "data-platform"}, {"Key": "env", "Value": "prod"}],
}

create_emr = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=EMR_CONFIG,
    aws_conn_id="aws_default",
    dag=dag,
)

TRANSFORM_STEPS = [
    {
        "Name": "Enrich Orders",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--driver-memory", "8g",
                "--executor-memory", "16g",
                "--executor-cores", "4",
                "--num-executors", "16",
                "--conf", "spark.sql.shuffle.partitions=400",
                "--conf", "spark.sql.adaptive.enabled=true",
                "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                "s3://shopmax-artifacts/spark/enrich_orders.py",
                "--date", "{{ ds }}",
                "--input-orders", "s3://shopmax-staging/orders/{{ ds }}/",
                "--input-products", "s3://shopmax-staging/products/{{ ds }}/",
                "--input-customers", "s3://shopmax-staging/customers/{{ ds }}/",
                "--output", "s3://shopmax-data-lake/silver/enriched_orders/date={{ ds }}/",
            ],
        },
    },
    {
        "Name": "Aggregate Revenue",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--driver-memory", "4g",
                "--executor-memory", "8g",
                "--num-executors", "8",
                "s3://shopmax-artifacts/spark/aggregate_revenue.py",
                "--date", "{{ ds }}",
                "--input", "s3://shopmax-data-lake/silver/enriched_orders/date={{ ds }}/",
                "--output", "s3://shopmax-data-lake/gold/revenue_metrics/date={{ ds }}/",
            ],
        },
    },
]

add_steps = EmrAddStepsOperator(
    task_id="add_transform_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    steps=TRANSFORM_STEPS,
    aws_conn_id="aws_default",
    dag=dag,
)

wait_transform = EmrStepSensor(
    task_id="wait_for_transform",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_transform_steps', key='return_value')[0] }}",
    timeout=7200,
    dag=dag,
)

terminate_emr = EmrTerminateJobFlowOperator(
    task_id="terminate_emr",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# ─── LOAD TO WAREHOUSE ───
load_warehouse = BashOperator(
    task_id="load_to_redshift",
    bash_command="""
        python /opt/airflow/dags/scripts/load_redshift.py             --date {{ ds }}             --source s3://shopmax-data-lake/gold/revenue_metrics/date={{ ds }}/             --target analytics.daily_revenue             --mode upsert --key order_date,merchant_id
    """,
    dag=dag,
)

# ─── NOTIFY ───
notify_success = SlackWebhookOperator(
    task_id="notify_success",
    slack_webhook_conn_id="slack_data_alerts",
    message=":white_check_mark: Daily orders pipeline complete for {{ ds }}",
    dag=dag,
)

# ─── DEPENDENCIES ───
wait_for_orders >> extract_orders
[extract_orders, extract_products, extract_customers] >> create_emr >> add_steps >> wait_transform >> terminate_emr >> load_warehouse >> notify_success
