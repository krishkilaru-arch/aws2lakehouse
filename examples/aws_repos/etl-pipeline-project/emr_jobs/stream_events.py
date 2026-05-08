"""
stream_events.py — Kafka structured streaming for e-commerce events.

Reads from: clickstream-events, purchase-events, cart-events
Writes to: s3://shopmax-data-lake/bronze/events/ (partitioned by event_type, hour)
"""
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-brokers", required=True)
    parser.add_argument("--topics", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--trigger-interval", default="30 seconds")
    parser.add_argument("--max-offsets", default="500000")
    args = parser.parse_args()
    
    spark = SparkSession.builder         .appName("ShopMaxEventStreaming")         .config("spark.sql.streaming.schemaInference", "true")         .getOrCreate()
    
    # Read from multiple Kafka topics
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_brokers)
        .option("subscribe", args.topics)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .load()
    )
    
    # Parse JSON payload
    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("properties", MapType(StringType(), StringType())),
        StructField("page_url", StringType()),
        StructField("device", StringType()),
        StructField("geo", StructType([
            StructField("country", StringType()),
            StructField("city", StringType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
        ])),
    ])
    
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str", "topic", "partition", "offset", "timestamp as kafka_ts")
        .withColumn("event", F.from_json("json_str", event_schema))
        .select("topic", "partition", "offset", "kafka_ts", "event.*")
        .withColumn("event_hour", F.date_format("timestamp", "yyyy-MM-dd-HH"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    
    # Write to S3 partitioned by event_type and hour
    query = (
        parsed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .option("path", args.output)
        .partitionBy("event_type", "event_hour")
        .trigger(processingTime=args.trigger_interval)
        .start()
    )
    
    query.awaitTermination()


if __name__ == "__main__":
    main()
