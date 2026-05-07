#!/bin/bash
# customer_churn_model.sh — Weekly ML model training on EMR
# Runs every Sunday at 2am on EMR cluster j-MLCLUSTER01
# Instance: 1x m5.4xlarge (master) + 5x p3.2xlarge (core, GPU)

spark-submit \
    --class com.acme.ml.ChurnModelTraining \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 5 \
    --executor-memory 32g \
    --executor-cores 8 \
    --driver-memory 8g \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.ml.tree.maxMemoryInMB=512 \
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
    --jars s3://acme-jars/xgboost4j-spark-1.7.jar,s3://acme-jars/acme-ml-utils-1.0.jar \
    s3://acme-jars/churn-model-trainer-3.1.jar \
    --training-data "s3://acme-data-lake/curated/customer_360/" \
    --features-data "s3://acme-data-lake/curated/session_analytics/" \
    --model-output "s3://acme-models/churn/latest/" \
    --metrics-output "s3://acme-models/churn/metrics/" \
    --date "{{ execution_date }}" \
    --validation-split 0.2
