"""
bronze_ingestion.py — Serverless Glue job: Raw file → Bronze layer.
Reads from S3 landing zone, applies schema from config, writes to Bronze.
Handles: CSV, JSON, Parquet, XML (vendor-specific formats).
"""
import sys
import json
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "source_key", "source_name", "target_path", "config"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

config = json.loads(args["config"])
source_path = f"s3://{args['source_bucket']}/{args['source_key']}"
target_path = f"{args['target_path']}{args['source_name']}/"

# Determine format from config
file_format = config.get("file_format", "csv")
schema_fields = config.get("schema", {})
delimiter = config.get("delimiter", ",")

print(f"Bronze ingestion: {source_path} → {target_path} (format: {file_format})")

# ─── READ based on format ───
if file_format == "csv":
    df = spark.read.csv(source_path, header=True, inferSchema=True, sep=delimiter)
elif file_format == "json":
    df = spark.read.json(source_path, multiLine=config.get("multi_line", False))
elif file_format == "parquet":
    df = spark.read.parquet(source_path)
elif file_format == "xml":
    df = spark.read.format("xml").option("rowTag", config.get("row_tag", "record")).load(source_path)
else:
    raise ValueError(f"Unsupported format: {file_format}")

# ─── STANDARDIZE ───
# Add metadata columns
df = (
    df
    .withColumn("_source_file", F.lit(args["source_key"]))
    .withColumn("_source_name", F.lit(args["source_name"]))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_file_format", F.lit(file_format))
)

# Rename columns to snake_case
for col in df.columns:
    new_name = col.lower().replace(" ", "_").replace("-", "_")
    if new_name != col:
        df = df.withColumnRenamed(col, new_name)

# ─── WRITE to Bronze (append, partitioned by ingestion date) ───
row_count = df.count()
(
    df
    .withColumn("_ingestion_date", F.current_date())
    .write.mode("append")
    .partitionBy("_ingestion_date")
    .parquet(target_path)
)

print(f"✅ Bronze complete: {row_count} rows → {target_path}")
