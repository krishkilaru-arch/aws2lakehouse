"""
realtime_events_pipeline.py — Process clickstream and purchase events from Kafka.

Runs continuously (EMR Streaming), restarts every 6 hours for checkpoint cleanup.
Source: Kafka topics (clickstream-events, purchase-events, cart-events)
Target: s3://shopmax-data-lake/bronze/events/
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "data-platform",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "realtime_events_pipeline",
    default_args=default_args,
    description="Kafka event streaming: clickstream + purchases + cart → bronze",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["streaming", "events", "kafka", "critical"],
)

STREAMING_STEP = {
    "Name": "StreamEvents",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--num-executors", "12",
            "--executor-memory", "8g",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "--conf", "spark.streaming.stopGracefullyOnShutdown=true",
            "--conf", "spark.sql.streaming.schemaInference=true",
            "s3://shopmax-artifacts/spark/stream_events.py",
            "--kafka-brokers", "b-1.shopmax-msk.kafka.us-east-1.amazonaws.com:9092",
            "--topics", "clickstream-events,purchase-events,cart-events",
            "--checkpoint", "s3://shopmax-checkpoints/events/",
            "--output", "s3://shopmax-data-lake/bronze/events/",
            "--trigger-interval", "30 seconds",
            "--max-offsets", "1000000",
        ],
    },
}

submit_streaming = EmrAddStepsOperator(
    task_id="submit_streaming_job",
    job_flow_id="j-STREAMING-CLUSTER-01",  # Long-running EMR cluster
    steps=[STREAMING_STEP],
    dag=dag,
)

# Health check
health_check = BashOperator(
    task_id="streaming_health_check",
    bash_command="""
        python /opt/airflow/dags/scripts/check_streaming_lag.py             --topics clickstream-events,purchase-events,cart-events             --max-lag-seconds 300             --alert-channel "#data-oncall"
    """,
    dag=dag,
)

submit_streaming >> health_check
