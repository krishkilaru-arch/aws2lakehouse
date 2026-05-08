#!/bin/bash
# market_data_feed.sh — Every-minute micro-batch from Kafka market data
# Instance: 1x m5.xlarge (master) + 2x r5.2xlarge (core)

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-memory 8g \
    --executor-cores 2 \
    --conf spark.sql.shuffle.partitions=100 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    s3://acme-jars/market-data-ingestor-1.5.jar \
    --kafka-brokers "b-1.acme-msk.kafka.us-east-1.amazonaws.com:9092" \
    --topic market-data-feed \
    --output-path s3://acme-data-lake/raw/market_data/ \
    --trigger-interval "1 minute" \
    --checkpoint s3://acme-checkpoints/market_data/
