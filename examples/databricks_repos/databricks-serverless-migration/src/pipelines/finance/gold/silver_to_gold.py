# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold: Aggregate vitals into patient health scores
# MAGIC
# MAGIC **Pipeline:** silver_to_gold
# MAGIC **Domain:** finance
# MAGIC **Layer:** gold
# MAGIC **Target table:** acme_prod.finance_gold.silver_to_gold
# MAGIC
# MAGIC Silver (curated records) → Gold (patient-level aggregates, risk scores, dashboards)
# MAGIC
# MAGIC Outputs:
# MAGIC - Patient daily summary (avg/min/max per vital per day)
# MAGIC - Risk scores (composite health score per patient)
# MAGIC - Lab summary (latest results, abnormal counts)

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("CATALOG", "acme_prod", "Catalog")
dbutils.widgets.text("SCHEMA", "finance_gold", "Schema")
dbutils.widgets.text("SOURCE_SYSTEM", "vitals", "Source System (vitals|lab_results)")
dbutils.widgets.text("FILE_DATE", "2024-01-01", "File Date (YYYY-MM-DD)")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
SOURCE_SYSTEM = dbutils.widgets.get("SOURCE_SYSTEM")
FILE_DATE = dbutils.widgets.get("FILE_DATE")

# Derived table names
DAILY_SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.patient_daily_summary"
RISK_SCORES_TABLE = f"{CATALOG}.{SCHEMA}.risk_scores"
LAB_SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.lab_summary"

# Silver source
SILVER_CATALOG = "acme_prod"
SILVER_SCHEMA = "finance_silver"

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Source System: {SOURCE_SYSTEM}")
print(f"  File Date: {FILE_DATE}")

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_to_gold")

# COMMAND ----------

# Read Silver data from Unity Catalog
silver_table = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{SOURCE_SYSTEM}"

silver_df = (
    spark.read
    .table(silver_table)
    .filter(F.col("date") == F.lit(FILE_DATE))
)

record_count = silver_df.count()
print(f"Silver → Gold: {record_count} records for {SOURCE_SYSTEM}/{FILE_DATE}")

if record_count == 0:
    dbutils.notebook.exit(f"No records found for {SOURCE_SYSTEM}/{FILE_DATE}. Exiting gracefully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patient Daily Summary (Vitals)

# COMMAND ----------

if SOURCE_SYSTEM == "vitals":
    # ─── PATIENT DAILY SUMMARY ───
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
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
    )

    print(f"Daily summary records: {daily_summary.count()}")
    daily_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Patient Daily Summary with Merge

# COMMAND ----------

if SOURCE_SYSTEM == "vitals":
    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DAILY_SUMMARY_TABLE} (
            patient_id STRING,
            measurement_type STRING,
            date DATE,
            avg_value DOUBLE,
            min_value DOUBLE,
            max_value DOUBLE,
            std_value DOUBLE,
            reading_count LONG,
            outlier_count LONG,
            last_reading_at TIMESTAMP,
            variability_index DOUBLE,
            _ingested_at TIMESTAMP,
            _source_system STRING
        )
        USING DELTA
        PARTITIONED BY (date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)

    # Merge/upsert pattern for idempotent writes
    target_table = DeltaTable.forName(spark, DAILY_SUMMARY_TABLE)

    (
        target_table.alias("target")
        .merge(
            daily_summary.alias("source"),
            """
            target.patient_id = source.patient_id
            AND target.measurement_type = source.measurement_type
            AND target.date = source.date
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ Merged patient_daily_summary for {FILE_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Scores

# COMMAND ----------

if SOURCE_SYSTEM == "vitals":
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
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
    )

    print(f"Risk score records: {risk_scores.count()}")
    risk_scores.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Risk Scores with Merge

# COMMAND ----------

if SOURCE_SYSTEM == "vitals":
    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RISK_SCORES_TABLE} (
            patient_id STRING,
            date DATE,
            avg_variability DOUBLE,
            total_outliers LONG,
            total_readings LONG,
            vitals_tracked LONG,
            risk_score DOUBLE,
            risk_category STRING,
            _ingested_at TIMESTAMP,
            _source_system STRING
        )
        USING DELTA
        PARTITIONED BY (date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)

    # Merge/upsert for idempotent writes
    target_table = DeltaTable.forName(spark, RISK_SCORES_TABLE)

    (
        target_table.alias("target")
        .merge(
            risk_scores.alias("source"),
            """
            target.patient_id = source.patient_id
            AND target.date = source.date
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ Merged risk_scores for {FILE_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Results Summary

# COMMAND ----------

if SOURCE_SYSTEM == "lab_results":
    # ─── LAB SUMMARY ───
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
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("_file_date", F.lit(FILE_DATE))
    )

    print(f"Lab summary records: {lab_summary.count()}")
    lab_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Lab Summary with Merge

# COMMAND ----------

if SOURCE_SYSTEM == "lab_results":
    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LAB_SUMMARY_TABLE} (
            patient_id STRING,
            test_code STRING,
            latest_result DOUBLE,
            avg_result DOUBLE,
            abnormal_count LONG,
            test_count LONG,
            latest_test_date TIMESTAMP,
            _ingested_at TIMESTAMP,
            _source_system STRING,
            _file_date STRING
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)

    # Merge/upsert for idempotent writes
    target_table = DeltaTable.forName(spark, LAB_SUMMARY_TABLE)

    (
        target_table.alias("target")
        .merge(
            lab_summary.alias("source"),
            """
            target.patient_id = source.patient_id
            AND target.test_code = source.test_code
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ Merged lab_summary for {FILE_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Write Optimization

# COMMAND ----------

# Optimize tables with ZORDER for common query patterns
if SOURCE_SYSTEM == "vitals":
    spark.sql(f"OPTIMIZE {DAILY_SUMMARY_TABLE} ZORDER BY (patient_id, measurement_type)")
    spark.sql(f"OPTIMIZE {RISK_SCORES_TABLE} ZORDER BY (patient_id, risk_category)")
    print(f"✅ Optimized {DAILY_SUMMARY_TABLE} and {RISK_SCORES_TABLE}")

elif SOURCE_SYSTEM == "lab_results":
    spark.sql(f"OPTIMIZE {LAB_SUMMARY_TABLE} ZORDER BY (patient_id, test_code)")
    print(f"✅ Optimized {LAB_SUMMARY_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

# Post-write validation
if SOURCE_SYSTEM == "vitals":
    # Validate daily summary
    ds_count = spark.table(DAILY_SUMMARY_TABLE).filter(F.col("date") == FILE_DATE).count()
    rs_count = spark.table(RISK_SCORES_TABLE).filter(F.col("date") == FILE_DATE).count()
    
    # Check for null risk scores
    null_risk = (
        spark.table(RISK_SCORES_TABLE)
        .filter((F.col("date") == FILE_DATE) & F.col("risk_score").isNull())
        .count()
    )
    
    print(f"Validation Results:")
    print(f"  Daily Summary records for {FILE_DATE}: {ds_count}")
    print(f"  Risk Score records for {FILE_DATE}: {rs_count}")
    print(f"  Null risk scores: {null_risk}")
    
    if null_risk > 0:
        logger.warning(f"⚠️ Found {null_risk} null risk scores for {FILE_DATE}")

elif SOURCE_SYSTEM == "lab_results":
    ls_count = spark.table(LAB_SUMMARY_TABLE).filter(F.col("_file_date") == FILE_DATE).count()
    print(f"Validation Results:")
    print(f"  Lab Summary records for {FILE_DATE}: {ls_count}")

# COMMAND ----------

# Final summary
result = {
    "status": "SUCCESS",
    "source_system": SOURCE_SYSTEM,
    "file_date": FILE_DATE,
    "source_records": record_count,
    "catalog": CATALOG,
    "schema": SCHEMA
}

print(f"\n{'='*60}")
print(f"✅ Silver → Gold pipeline completed successfully")
print(f"   Source: {SOURCE_SYSTEM} | Date: {FILE_DATE}")
print(f"   Records processed: {record_count}")
print(f"{'='*60}")

dbutils.notebook.exit(str(result))
