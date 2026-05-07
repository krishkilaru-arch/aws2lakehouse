"""
bronze_to_silver.py — Glue Job: Transform raw vitals/labs into curated format.

Bronze (raw JSONL) → Silver (Parquet, deduplicated, typed, partitioned)

Transformations:
  - Schema enforcement and type casting
  - Deduplication (patient_id + timestamp + measurement_type)
  - Outlier flagging (statistical + clinical ranges)
  - Time-series alignment (15-min buckets)
  - Partitioning by source_system, date
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_system", "file_date", "input_path", "run_id"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_system = args["source_system"]
file_date = args["file_date"]
input_path = f"s3://medstream-datalake-prod/bronze/{source_system}/date={file_date}/"
output_path = f"s3://medstream-datalake-prod/silver/{source_system}/date={file_date}/"

print(f"Bronze → Silver: {source_system} for {file_date}")

# ─── READ BRONZE ───
raw_df = spark.read.json(input_path)
print(f"Read {raw_df.count()} records from Bronze")

# ─── SCHEMA ENFORCEMENT ───
if source_system == "vitals":
    typed_df = (
        raw_df
        .withColumn("patient_id", F.col("patient_id").cast(StringType()))
        .withColumn("device_id", F.col("device_id").cast(StringType()))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("measurement_type", F.col("measurement_type").cast(StringType()))
        .withColumn("value", F.col("value").cast(DoubleType()))
        .withColumn("unit", F.col("unit").cast(StringType()))
    )
elif source_system == "lab_results":
    typed_df = (
        raw_df
        .withColumn("patient_id", F.col("patient_id").cast(StringType()))
        .withColumn("test_code", F.col("test_code").cast(StringType()))
        .withColumn("result_value", F.col("result_value").cast(DoubleType()))
        .withColumn("reference_range_low", F.col("reference_range_low").cast(DoubleType()))
        .withColumn("reference_range_high", F.col("reference_range_high").cast(DoubleType()))
        .withColumn("collected_at", F.to_timestamp("collected_at"))
        .withColumn("is_abnormal", 
            (F.col("result_value") < F.col("reference_range_low")) |
            (F.col("result_value") > F.col("reference_range_high")))
    )
else:
    typed_df = raw_df

# ─── DEDUPLICATION ───
window = Window.partitionBy("patient_id", "timestamp", "measurement_type").orderBy(F.desc("_enriched_at"))
deduped_df = (
    typed_df
    .withColumn("_row_num", F.row_number().over(window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
dup_count = raw_df.count() - deduped_df.count()
print(f"Removed {dup_count} duplicates")

# ─── OUTLIER DETECTION (statistical) ───
if source_system == "vitals":
    stats_window = Window.partitionBy("patient_id", "measurement_type").orderBy("timestamp").rowsBetween(-100, -1)
    deduped_df = (
        deduped_df
        .withColumn("rolling_mean", F.avg("value").over(stats_window))
        .withColumn("rolling_std", F.stddev("value").over(stats_window))
        .withColumn("z_score", 
            F.when(F.col("rolling_std") > 0,
                   (F.col("value") - F.col("rolling_mean")) / F.col("rolling_std"))
            .otherwise(F.lit(0.0)))
        .withColumn("is_outlier", F.abs(F.col("z_score")) > 3.0)
    )

# ─── TIME BUCKET ALIGNMENT (15-min) ───
deduped_df = deduped_df.withColumn(
    "time_bucket", 
    F.window("timestamp", "15 minutes").getField("start")
)

# ─── ADD SILVER METADATA ───
silver_df = (
    deduped_df
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_source_system", F.lit(source_system))
    .withColumn("_file_date", F.lit(file_date))
)

# ─── WRITE SILVER ───
silver_df.write     .mode("overwrite")     .partitionBy("measurement_type")     .parquet(output_path)

print(f"✅ Silver write complete: {silver_df.count()} records to {output_path}")
job.commit()
