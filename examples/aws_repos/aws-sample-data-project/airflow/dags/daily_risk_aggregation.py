"""
daily_risk_aggregation.py — Airflow DAG for daily risk calculations.
Runs at 6am ET, depends on trade_events and market_data completing.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "risk-engineering",
    "depends_on_past": False,
    "email": ["risk-oncall@acmecapital.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "daily_risk_aggregation",
    default_args=default_args,
    description="Aggregate trade and market data for daily risk metrics",
    schedule_interval="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["risk", "daily", "critical"],
)

# Wait for upstream data to land
wait_for_trades = S3KeySensor(
    task_id="wait_for_trades",
    bucket_name="acme-data-lake",
    bucket_key="raw/trade_events/dt={{ ds }}/",
    aws_conn_id="aws_default",
    timeout=3600,
    dag=dag,
)

wait_for_market_data = S3KeySensor(
    task_id="wait_for_market_data",
    bucket_name="acme-data-lake",
    bucket_key="raw/market_data/dt={{ ds }}/",
    aws_conn_id="aws_default",
    timeout=3600,
    dag=dag,
)

# EMR Step: Bronze → Silver (clean and validate)
validate_trades = EmrAddStepsOperator(
    task_id="validate_trades",
    job_flow_id="j-RISKCLUSTER01",
    steps=[{
        "Name": "Validate Trade Data",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--class", "com.acme.risk.ValidateTrades",
                "--master", "yarn",
                "--deploy-mode", "cluster",
                "--num-executors", "20",
                "--executor-memory", "8g",
                "--conf", "spark.sql.shuffle.partitions=400",
                "s3://acme-jars/risk-etl-2.3.1.jar",
                "--date", "{{ ds }}",
                "--input", "s3://acme-data-lake/raw/trade_events/dt={{ ds }}/",
                "--output", "s3://acme-data-lake/curated/trades/dt={{ ds }}/",
            ],
        },
    }],
    aws_conn_id="aws_default",
    dag=dag,
)

# EMR Step: Silver → Gold (aggregate risk metrics)
compute_var = EmrAddStepsOperator(
    task_id="compute_var",
    job_flow_id="j-RISKCLUSTER01",
    steps=[{
        "Name": "Compute VaR and Exposure",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--class", "com.acme.risk.ComputeVaR",
                "--master", "yarn",
                "--deploy-mode", "cluster",
                "--num-executors", "40",
                "--executor-memory", "16g",
                "--conf", "spark.sql.adaptive.enabled=true",
                "s3://acme-jars/risk-etl-2.3.1.jar",
                "--date", "{{ ds }}",
                "--trades", "s3://acme-data-lake/curated/trades/dt={{ ds }}/",
                "--market-data", "s3://acme-data-lake/curated/market_data/dt={{ ds }}/",
                "--output", "s3://acme-data-lake/aggregated/risk_metrics/dt={{ ds }}/",
            ],
        },
    }],
    aws_conn_id="aws_default",
    dag=dag,
)

# Load to Redshift for dashboards
load_redshift = BashOperator(
    task_id="load_redshift",
    bash_command="""
        aws redshift-data execute-statement \
            --cluster-identifier acme-analytics \
            --database risk_db \
            --sql "COPY risk_metrics FROM 's3://acme-data-lake/aggregated/risk_metrics/dt={{ ds }}/' IAM_ROLE 'arn:aws:iam::123456:role/RedshiftCopy' FORMAT PARQUET;"
    """,
    dag=dag,
)

# Notify
notify_complete = PythonOperator(
    task_id="notify_complete",
    python_callable=lambda: print("Risk aggregation complete"),
    dag=dag,
)

# Dependencies
[wait_for_trades, wait_for_market_data] >> validate_trades >> compute_var >> load_redshift >> notify_complete
