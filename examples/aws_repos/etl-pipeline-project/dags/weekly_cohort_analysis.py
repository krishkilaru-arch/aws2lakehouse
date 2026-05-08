"""
weekly_cohort_analysis.py — Weekly customer cohort analysis and churn prediction.

Runs every Monday at 5 AM. Heavy computation (30+ GB joins).
Source: Silver layer (enriched_orders, customer_profiles)
Target: Gold layer (cohort_metrics, churn_scores)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "analytics-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "weekly_cohort_analysis",
    default_args=default_args,
    description="Weekly: customer cohorts, LTV calculation, churn prediction",
    schedule_interval="0 5 * * 1",
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["weekly", "analytics", "ml", "cohorts"],
)

cohort_step = {
    "Name": "CohortAnalysis",
    "ActionOnFailure": "TERMINATE_CLUSTER",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--num-executors", "20",
            "--executor-memory", "16g",
            "--driver-memory", "8g",
            "--conf", "spark.sql.shuffle.partitions=800",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
            "--py-files", "s3://shopmax-artifacts/libs/shopmax_analytics-1.5.0.whl",
            "s3://shopmax-artifacts/spark/cohort_analysis.py",
            "--week-start", "{{ ds }}",
            "--lookback-weeks", "52",
            "--output-cohorts", "s3://shopmax-data-lake/gold/cohort_metrics/",
            "--output-churn", "s3://shopmax-data-lake/gold/churn_scores/",
        ],
    },
}

run_cohorts = EmrAddStepsOperator(
    task_id="run_cohort_analysis",
    job_flow_id="j-ANALYTICS-CLUSTER-01",
    steps=[cohort_step],
    dag=dag,
)
