"""
silver_transformation.py — Serverless Glue job: Bronze → Silver.
Applies business transforms defined in DynamoDB config:
  - Column type casting
  - Business logic (derived columns)
  - Deduplication
  - Joins with reference data
  - PII masking
"""
import sys
import json
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "source_name", "target_path", "config"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

config = json.loads(args["config"])
source_path = f"{args['source_path']}{args['source_name']}/"
target_path = f"{args['target_path']}{args['source_name']}/"

print(f"Silver transform: {source_path} → {target_path}")

# ─── READ Bronze ───
df = spark.read.parquet(source_path)

# ─── Apply transforms from config ───
transforms = config.get("silver_transforms", [])

for transform in transforms:
    t_type = transform["type"]
    
    if t_type == "cast":
        for col, dtype in transform["columns"].items():
            df = df.withColumn(col, F.col(col).cast(dtype))
    
    elif t_type == "derive":
        for col, expr in transform["expressions"].items():
            df = df.withColumn(col, F.expr(expr))
    
    elif t_type == "dedup":
        key_cols = transform["key_columns"]
        order_col = transform["order_by"]
        window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
        df = df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")
    
    elif t_type == "filter":
        df = df.filter(transform["condition"])
    
    elif t_type == "mask":
        for col in transform["columns"]:
            strategy = transform.get("strategy", "hash")
            if strategy == "hash":
                df = df.withColumn(col, F.sha2(F.col(col).cast("string"), 256))
            elif strategy == "redact":
                df = df.withColumn(col, F.lit("[MASKED]"))
    
    elif t_type == "rename":
        for old_name, new_name in transform["mapping"].items():
            df = df.withColumnRenamed(old_name, new_name)

# ─── Add silver metadata ───
df = (
    df
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_silver_version", F.lit(config.get("version", "1.0")))
)

# ─── WRITE Silver ───
row_count = df.count()
partition_cols = config.get("silver_partition_by", ["_ingestion_date"])

df.write.mode("overwrite").partitionBy(*partition_cols).parquet(target_path)

print(f"✅ Silver complete: {row_count} rows → {target_path}")
