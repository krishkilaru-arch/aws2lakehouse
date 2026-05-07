#!/bin/bash
# trade_events_streaming.sh — EMR streaming job for trade events from Kafka
# Runs continuously on dedicated EMR cluster j-RISKSTREAMING01
# Instance: 1x m5.2xlarge (master) + 4x r5.4xlarge (core) + 0-8 c5.2xlarge (task/spot)

spark-submit \
    --class com.acme.risk.TradeEventProcessor \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 8 \
    --executor-memory 12g \
    --executor-cores 4 \
    --driver-memory 4g \
    --conf spark.streaming.kafka.maxRatePerPartition=10000 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=4 \
    --conf spark.dynamicAllocation.maxExecutors=16 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    s3://acme-jars/risk-streaming-processor-2.3.1.jar \
    --kafka-brokers "b-1.acme-msk.kafka.us-east-1.amazonaws.com:9092,b-2.acme-msk.kafka.us-east-1.amazonaws.com:9092" \
    --topic trade-events-v2 \
    --checkpoint-location s3://acme-checkpoints/trade_events/ \
    --output-path s3://acme-data-lake/raw/trade_events/ \
    --trigger-interval "30 seconds" \
    --starting-offsets latest
