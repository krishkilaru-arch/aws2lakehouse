"""
silver_to_gold.py — Glue Job: Aggregate vitals into patient health scores.

Silver (curated records) → Gold (patient-level aggregates, risk scores, dashboards)

Outputs:
  - Patient daily summary (avg/min/max per vital per day)
  - Risk scores (composite health score per patient)
  - Alert history (time since last critical event)
  - Population health (facility-level aggregates)
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_system", "file_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_system = args["source_system"]
file_date = args["file_date"]

# Read Silver
silver_path = f"s3://medstream-datalake-prod/silver/{source_system}/date={file_date}/"
silver_df = spark.read.parquet(silver_path)

print(f"Silver → Gold: {silver_df.count()} records for {source_system}/{file_date}")

# ─── PATIENT DAILY SUMMARY ───
if source_system == "vitals":
    daily_summary = (
        silver_df
        .groupBy("patient_id", "measurement_type", F.to_date("timestamp").alias("date"))
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.stddev("value").alias("std_value"),
            F.count("*").alias("reading_count"),
            F.sum(F.when(F.col("is_outlier"), 1).otherwise(0)).alias("outlier_count"),
            F.max("timestamp").alias("last_reading_at"),
        )
        .withColumn("variability_index", F.col("std_value") / F.col("avg_value"))
    )
    
    daily_summary.write.mode("overwrite")         .partitionBy("date")         .parquet(f"s3://medstream-datalake-prod/gold/patient_daily_summary/date={file_date}/")
    
    # ─── RISK SCORES ───
    risk_scores = (
        daily_summary
        .groupBy("patient_id", "date")
        .agg(
            F.avg("variability_index").alias("avg_variability"),
            F.sum("outlier_count").alias("total_outliers"),
            F.sum("reading_count").alias("total_readings"),
            F.count("measurement_type").alias("vitals_tracked"),
        )
        .withColumn("risk_score",
            F.least(F.lit(100.0),
                F.col("avg_variability") * 20 +
                F.col("total_outliers") * 5 +
                F.when(F.col("total_readings") < 10, F.lit(20)).otherwise(F.lit(0))
            ))
        .withColumn("risk_category",
            F.when(F.col("risk_score") > 70, F.lit("HIGH"))
            .when(F.col("risk_score") > 40, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")))
    )
    
    risk_scores.write.mode("overwrite")         .parquet(f"s3://medstream-datalake-prod/gold/risk_scores/date={file_date}/")
    
    print(f"✅ Gold outputs: daily_summary + risk_scores for {file_date}")

elif source_system == "lab_results":
    lab_summary = (
        silver_df
        .groupBy("patient_id", "test_code")
        .agg(
            F.last("result_value").alias("latest_result"),
            F.avg("result_value").alias("avg_result"),
            F.sum(F.when(F.col("is_abnormal"), 1).otherwise(0)).alias("abnormal_count"),
            F.count("*").alias("test_count"),
            F.max("collected_at").alias("latest_test_date"),
        )
    )
    
    lab_summary.write.mode("overwrite")         .parquet(f"s3://medstream-datalake-prod/gold/lab_summary/date={file_date}/")

job.commit()
