"""
gold_aggregation.py — Serverless Glue job: Silver → Gold.
Builds aggregated views, KPIs, and reporting tables.
Config-driven: aggregation specs come from DynamoDB.
"""
import sys
import json
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "source_name", "target_path", "config"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

config = json.loads(args["config"])
source_path = f"{args['source_path']}{args['source_name']}/"
target_path = f"{args['target_path']}{args['source_name']}/"

print(f"Gold aggregation: {source_path} → {target_path}")

# ─── READ Silver ───
df = spark.read.parquet(source_path)

# ─── Apply aggregations from config ───
agg_specs = config.get("gold_aggregations", [])

for spec in agg_specs:
    agg_name = spec["name"]
    group_cols = spec["group_by"]
    agg_output_path = f"{target_path}{agg_name}/"
    
    # Build aggregation expressions
    agg_exprs = []
    for metric in spec["metrics"]:
        col = metric["column"]
        func = metric["function"]
        alias = metric.get("alias", f"{func}_{col}")
        
        if func == "sum":
            agg_exprs.append(F.sum(col).alias(alias))
        elif func == "avg":
            agg_exprs.append(F.avg(col).alias(alias))
        elif func == "count":
            agg_exprs.append(F.count(col).alias(alias))
        elif func == "count_distinct":
            agg_exprs.append(F.countDistinct(col).alias(alias))
        elif func == "max":
            agg_exprs.append(F.max(col).alias(alias))
        elif func == "min":
            agg_exprs.append(F.min(col).alias(alias))
    
    # Execute aggregation
    agg_df = df.groupBy(*group_cols).agg(*agg_exprs)
    
    # Apply post-aggregation filters
    if "having" in spec:
        agg_df = agg_df.filter(spec["having"])
    
    # Write
    agg_df.withColumn("_gold_created_at", F.current_timestamp()).write.mode("overwrite").parquet(agg_output_path)
    print(f"  Gold/{agg_name}: {agg_df.count()} rows")

print(f"✅ Gold layer complete")
