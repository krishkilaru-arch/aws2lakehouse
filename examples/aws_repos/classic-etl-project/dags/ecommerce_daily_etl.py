"""
ecommerce_daily_etl.py — Main daily ETL orchestration.
Extracts from 5 source systems, transforms in EMR, loads to Redshift star schema.

Sources:
  - PostgreSQL (orders, customers, products)
  - MySQL (inventory, warehouses)
  - REST API (shipping carriers: FedEx, UPS, DHL status)
  - S3 files (clickstream logs, marketing attribution CSVs)
  - Salesforce (CRM contacts via JDBC)

Target: Redshift data warehouse (star schema)
  - fact_orders, fact_returns, fact_page_views
  - dim_customer, dim_product, dim_date, dim_geography

Schedule: Daily at 3:00 AM UTC | SLA: Complete by 6:00 AM UTC
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,
    "email": ["data-alerts@ecomco.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
    "sla": timedelta(hours=3),
}

dag = DAG(
    "ecommerce_daily_etl",
    default_args=default_args,
    description="Daily ETL: 5 sources → EMR transform → Redshift star schema",
    schedule_interval="0 3 * * *",
    start_date=datetime(2023, 6, 1),
    catchup=True,
    max_active_runs=1,
    tags=["ecommerce", "daily", "critical", "star-schema"],
)

# ═══ PHASE 1: EXTRACTION ═══
with TaskGroup("extract", dag=dag) as extract_group:

    # Wait for clickstream files to land
    wait_for_clickstream = S3KeySensor(
        task_id="wait_clickstream_files",
        bucket_name="ecomco-raw-data",
        bucket_key="clickstream/{{ ds }}/",
        wildcard_match=True,
        timeout=3600,
        poke_interval=120,
        dag=dag,
    )

    # Extract from PostgreSQL orders DB
    extract_orders = BashOperator(
        task_id="extract_orders_postgres",
        bash_command="""
            python /opt/airflow/scripts/extract_jdbc.py \
                --source postgres \
                --connection orders_postgres \
                --tables orders,order_items,returns \
                --incremental-column updated_at \
                --since {{ ds }} \
                --output s3://ecomco-staging/extract/orders/{{ ds }}/
        """,
        dag=dag,
    )

    # Extract from MySQL inventory
    extract_inventory = BashOperator(
        task_id="extract_inventory_mysql",
        bash_command="""
            python /opt/airflow/scripts/extract_jdbc.py \
                --source mysql \
                --connection inventory_mysql \
                --tables inventory_snapshots,warehouses,shipments \
                --full-load \
                --output s3://ecomco-staging/extract/inventory/{{ ds }}/
        """,
        dag=dag,
    )

    # Extract shipping status from carrier APIs
    extract_shipping_fedex = SimpleHttpOperator(
        task_id="extract_shipping_fedex",
        http_conn_id="fedex_api",
        endpoint="/track/v1/shipments?from_date={{ ds }}",
        method="GET",
        response_filter=lambda r: r.json(),
        log_response=True,
        dag=dag,
    )

    # Extract marketing attribution (CSV drops from ad platforms)
    extract_marketing = BashOperator(
        task_id="extract_marketing_csvs",
        bash_command="""
            aws s3 sync s3://ecomco-marketing-drops/{{ ds }}/ \
                s3://ecomco-staging/extract/marketing/{{ ds }}/
        """,
        dag=dag,
    )

    # Extract Salesforce CRM
    extract_crm = BashOperator(
        task_id="extract_salesforce_crm",
        bash_command="""
            python /opt/airflow/scripts/extract_salesforce.py \
                --objects Contact,Opportunity,Account \
                --since {{ ds }} \
                --output s3://ecomco-staging/extract/crm/{{ ds }}/
        """,
        dag=dag,
    )

# ═══ PHASE 2: SPARK TRANSFORMATION (EMR) ═══
EMR_CLUSTER_CONFIG = {
    "Name": "ecomco-etl-{{ ds }}",
    "ReleaseLabel": "emr-6.12.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hive"}],
    "Instances": {
        "MasterInstanceType": "m5.2xlarge",
        "SlaveInstanceType": "r5.4xlarge",
        "InstanceCount": 6,
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "BootstrapActions": [
        {"Name": "install-deps", "ScriptBootstrapAction": {
            "Path": "s3://ecomco-scripts/bootstrap/install_deps.sh"
        }}
    ],
    "Configurations": [
        {"Classification": "spark-defaults", "Properties": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        }}
    ],
}

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=EMR_CLUSTER_CONFIG,
    aws_conn_id="aws_default",
    dag=dag,
)

# Step 1: Staging to Bronze (clean + standardize)
bronze_step = EmrAddStepsOperator(
    task_id="emr_bronze_layer",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    steps=[{
        "Name": "Bronze Layer Processing",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                     "--num-executors", "10", "--executor-memory", "8g",
                     "s3://ecomco-scripts/emr/bronze_processing.py",
                     "--date", "{{ ds }}"]
        }
    }],
    dag=dag,
)

# Step 2: Bronze to Silver (transform + join)
silver_step = EmrAddStepsOperator(
    task_id="emr_silver_layer",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    steps=[{
        "Name": "Silver Layer Processing",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                     "--num-executors", "20", "--executor-memory", "16g",
                     "--conf", "spark.sql.shuffle.partitions=400",
                     "s3://ecomco-scripts/emr/silver_processing.py",
                     "--date", "{{ ds }}"]
        }
    }],
    dag=dag,
)

# Step 3: Silver to Gold (aggregate for star schema)
gold_step = EmrAddStepsOperator(
    task_id="emr_gold_layer",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    steps=[{
        "Name": "Gold Layer Processing",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                     "--num-executors", "15", "--executor-memory", "12g",
                     "s3://ecomco-scripts/emr/gold_star_schema.py",
                     "--date", "{{ ds }}"]
        }
    }],
    dag=dag,
)

wait_gold = EmrStepSensor(
    task_id="wait_gold_complete",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='emr_gold_layer')[0] }}",
    dag=dag,
)

terminate_emr = EmrTerminateJobFlowOperator(
    task_id="terminate_emr",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    dag=dag,
)

# ═══ PHASE 3: LOAD TO REDSHIFT ═══
with TaskGroup("load_redshift", dag=dag) as load_group:
    
    load_fact_orders = S3ToRedshiftOperator(
        task_id="load_fact_orders",
        schema="analytics",
        table="fact_orders",
        s3_bucket="ecomco-data-lake",
        s3_key="gold/fact_orders/{{ ds }}/",
        redshift_conn_id="redshift_default",
        copy_options=["FORMAT PARQUET"],
        method="UPSERT",
        upsert_keys=["order_id", "order_date"],
        dag=dag,
    )

    load_dim_customer = S3ToRedshiftOperator(
        task_id="load_dim_customer",
        schema="analytics",
        table="dim_customer",
        s3_bucket="ecomco-data-lake",
        s3_key="gold/dim_customer/latest/",
        redshift_conn_id="redshift_default",
        copy_options=["FORMAT PARQUET"],
        method="REPLACE",
        dag=dag,
    )

    load_dim_product = S3ToRedshiftOperator(
        task_id="load_dim_product",
        schema="analytics",
        table="dim_product",
        s3_bucket="ecomco-data-lake",
        s3_key="gold/dim_product/latest/",
        redshift_conn_id="redshift_default",
        copy_options=["FORMAT PARQUET"],
        method="REPLACE",
        dag=dag,
    )

# ═══ PHASE 4: VALIDATION ═══
validate_counts = BashOperator(
    task_id="validate_row_counts",
    bash_command="""
        python /opt/airflow/scripts/validate_load.py \
            --date {{ ds }} \
            --tables fact_orders,dim_customer,dim_product \
            --threshold 0.95
    """,
    dag=dag,
)

# ═══ DEPENDENCIES ═══
extract_group >> create_emr_cluster >> bronze_step >> silver_step >> gold_step >> wait_gold >> terminate_emr >> load_group >> validate_counts
