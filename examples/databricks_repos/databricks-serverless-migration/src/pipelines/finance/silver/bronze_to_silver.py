# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: Transform Raw Vitals/Labs into Curated Format
# MAGIC
# MAGIC **Pipeline:** bronze_to_silver
# MAGIC **Domain:** finance
# MAGIC **Layer:** silver
# MAGIC **Target:** acme_prod.finance_silver.bronze_to_silver
# MAGIC
# MAGIC ## Transformations:
# MAGIC - Schema enforcement and type casting
# MAGIC - Deduplication (patient_id + timestamp + measurement_type)
# MAGIC - Outlier flagging (statistical + clinical ranges)
# MAGIC - Time-series alignment (15-min buckets)
# MAGIC - Partitioned by source_system, date

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("CATALOG", "acme_prod", "Unity Catalog")
dbutils.widgets.text("SCHEMA", "finance_silver", "Target Schema")
dbutils.widgets.text("TABLE", "bronze_to_silver", "Target Table")
dbutils.widgets.text("SOURCE_SYSTEM", "vitals", "Source System (vitals|lab_results)")
dbutils.widgets.text("FILE_DATE", "", "File Date (yyyy-MM-dd)")
dbutils.widgets.text("RUN_ID", "", "Pipeline Run ID")

# COMMAND ----------

# DBTITLE 1,Configuration and Imports
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StringType, DoubleType, TimestampType, BooleanType, StructType, StructField
)
from delta.tables import DeltaTable
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_to_silver")

# Resolve parameters
CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
TABLE = dbutils.widgets.get("TABLE")
SOURCE_SYSTEM = dbutils.widgets.get("SOURCE_SYSTEM")
FILE_DATE = dbutils.widgets.get("FILE_DATE")
RUN_ID = dbutils.widgets.get("RUN_ID")

# Fully qualified table name
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Unity Catalog Volume paths (replaces s3:// paths)
BRONZE_VOLUME_PATH = f"/Volumes/{CATALOG}/finance_bronze/medstream_raw/{SOURCE_SYSTEM}/date={FILE_DATE}/"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/_checkpoints/{TABLE}/{SOURCE_SYSTEM}"

logger.info(f"Bronze → Silver: {SOURCE_SYSTEM} for {FILE_DATE}")
logger.info(f"Target table: {TARGET_TABLE}")
logger.info(f"Source path: {BRONZE_VOLUME_PATH}")
logger.info(f"Run ID: {RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Set Unity Catalog Context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create Target Table if Not Exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    patient_id STRING,
    device_id STRING,
    timestamp TIMESTAMP,
    measurement_type STRING,
    value DOUBLE,
    unit STRING,
    test_code STRING,
    result_value DOUBLE,
    reference_range_low DOUBLE,
    reference_range_high DOUBLE,
    collected_at TIMESTAMP,
    is_abnormal BOOLEAN,
    rolling_mean DOUBLE,
    rolling_std DOUBLE,
    z_score DOUBLE,
    is_outlier BOOLEAN,
    time_bucket TIMESTAMP,
    _silver_processed_at TIMESTAMP,
    _source_system STRING,
    _file_date STRING,
    _ingested_at TIMESTAMP,
    _source_file STRING,
    _run_id STRING
)
USING DELTA
PARTITIONED BY (measurement_type)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'silver',
    'domain' = 'finance',
    'pipeline' = 'bronze_to_silver'
)
""")

logger.info(f"Target table {TARGET_TABLE} is ready")

# COMMAND ----------

# DBTITLE 1,Read Bronze Data with Auto Loader (Incremental Ingestion)
# Use Auto Loader with cloudFiles for incremental file ingestion with schema evolution
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/_schema")
    .load(BRONZE_VOLUME_PATH)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)

logger.info("Auto Loader stream configured for Bronze ingestion")

# COMMAND ----------

# DBTITLE 1,Transformation Functions

def enforce_schema_vitals(df):
    """Schema enforcement and type casting for vitals data."""
    return (
        df
        .withColumn("patient_id", F.col("patient_id").cast(StringType()))
        .withColumn("device_id", F.col("device_id").cast(StringType()))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("measurement_type", F.col("measurement_type").cast(StringType()))
        .withColumn("value", F.col("value").cast(DoubleType()))
        .withColumn("unit", F.col("unit").cast(StringType()))
        # Null out lab-specific columns for vitals
        .withColumn("test_code", F.lit(None).cast(StringType()))
        .withColumn("result_value", F.lit(None).cast(DoubleType()))
        .withColumn("reference_range_low", F.lit(None).cast(DoubleType()))
        .withColumn("reference_range_high", F.lit(None).cast(DoubleType()))
        .withColumn("collected_at", F.lit(None).cast(TimestampType()))
        .withColumn("is_abnormal", F.lit(None).cast(BooleanType()))
    )


def enforce_schema_lab_results(df):
    """Schema enforcement and type casting for lab results data."""
    return (
        df
        .withColumn("patient_id", F.col("patient_id").cast(StringType()))
        .withColumn("test_code", F.col("test_code").cast(StringType()))
        .withColumn("result_value", F.col("result_value").cast(DoubleType()))
        .withColumn("reference_range_low", F.col("reference_range_low").cast(DoubleType()))
        .withColumn("reference_range_high", F.col("reference_range_high").cast(DoubleType()))
        .withColumn("collected_at", F.to_timestamp("collected_at"))
        .withColumn("is_abnormal",
            (F.col("result_value") < F.col("reference_range_low")) |
            (F.col("result_value") > F.col("reference_range_high"))
        )
        # Map lab timestamp to common timestamp field for dedup/bucketing
        .withColumn("timestamp", F.to_timestamp("collected_at"))
        .withColumn("measurement_type", F.col("test_code"))
        # Null out vitals-specific columns
        .withColumn("device_id", F.lit(None).cast(StringType()))
        .withColumn("value", F.col("result_value"))
        .withColumn("unit", F.lit(None).cast(StringType()))
    )


def deduplicate(df):
    """Deduplicate by patient_id + timestamp + measurement_type, keeping latest enrichment."""
    window = Window.partitionBy(
        "patient_id", "timestamp", "measurement_type"
    ).orderBy(F.desc("_ingested_at"))
    
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def detect_outliers_vitals(df):
    """Statistical outlier detection using rolling z-score for vitals."""
    stats_window = (
        Window.partitionBy("patient_id", "measurement_type")
        .orderBy("timestamp")
        .rowsBetween(-100, -1)
    )
    return (
        df
        .withColumn("rolling_mean", F.avg("value").over(stats_window))
        .withColumn("rolling_std", F.stddev("value").over(stats_window))
        .withColumn("z_score",
            F.when(F.col("rolling_std") > 0,
                   (F.col("value") - F.col("rolling_mean")) / F.col("rolling_std"))
            .otherwise(F.lit(0.0))
        )
        .withColumn("is_outlier", F.abs(F.col("z_score")) > 3.0)
    )


def align_time_buckets(df):
    """Align timestamps to 15-minute buckets."""
    return df.withColumn(
        "time_bucket",
        F.window("timestamp", "15 minutes").getField("start")
    )


def add_silver_metadata(df, source_system, file_date, run_id):
    """Add silver layer metadata columns."""
    return (
        df
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_file_date", F.lit(file_date))
        .withColumn("_run_id", F.lit(run_id))
    )

# COMMAND ----------

# DBTITLE 1,foreachBatch Processing with DQ Checks and Merge

def process_micro_batch(batch_df, batch_id):
    """
    Process each micro-batch: schema enforcement, dedup, outlier detection,
    time alignment, DQ checks, and merge into Delta.
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: Empty batch, skipping")
        return
    
    record_count = batch_df.count()
    logger.info(f"Batch {batch_id}: Processing {record_count} records from Bronze")
    
    # ─── SCHEMA ENFORCEMENT ───
    if SOURCE_SYSTEM == "vitals":
        typed_df = enforce_schema_vitals(batch_df)
    elif SOURCE_SYSTEM == "lab_results":
        typed_df = enforce_schema_lab_results(batch_df)
    else:
        typed_df = batch_df
        logger.warning(f"Unknown source_system: {SOURCE_SYSTEM}, applying no schema enforcement")
    
    # ─── DEDUPLICATION ───
    deduped_df = deduplicate(typed_df)
    deduped_count = deduped_df.count()
    dup_count = record_count - deduped_count
    logger.info(f"Batch {batch_id}: Removed {dup_count} duplicates")
    
    # ─── OUTLIER DETECTION ───
    if SOURCE_SYSTEM == "vitals":
        enriched_df = detect_outliers_vitals(deduped_df)
    else:
        enriched_df = (
            deduped_df
            .withColumn("rolling_mean", F.lit(None).cast(DoubleType()))
            .withColumn("rolling_std", F.lit(None).cast(DoubleType()))
            .withColumn("z_score", F.lit(None).cast(DoubleType()))
            .withColumn("is_outlier", F.lit(None).cast(BooleanType()))
        )
    
    # ─── TIME BUCKET ALIGNMENT ───
    bucketed_df = align_time_buckets(enriched_df)
    
    # ─── ADD SILVER METADATA ───
    silver_df = add_silver_metadata(bucketed_df, SOURCE_SYSTEM, FILE_DATE, RUN_ID)
    
    # ─── DATA QUALITY CHECKS ───
    total_records = silver_df.count()
    null_patient_ids = silver_df.filter(F.col("patient_id").isNull()).count()
    null_timestamps = silver_df.filter(F.col("timestamp").isNull()).count()
    
    dq_pass = True
    if null_patient_ids > 0:
        logger.warning(f"Batch {batch_id}: {null_patient_ids} records with NULL patient_id")
        if null_patient_ids / total_records > 0.1:  # >10% null = fail
            dq_pass = False
    
    if null_timestamps > 0:
        logger.warning(f"Batch {batch_id}: {null_timestamps} records with NULL timestamp")
        if null_timestamps / total_records > 0.1:
            dq_pass = False
    
    if not dq_pass:
        error_msg = f"Batch {batch_id}: Data quality check FAILED. Aborting batch."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"Batch {batch_id}: DQ checks passed ({total_records} records)")
    
    # ─── Filter out null keys before merge ───
    clean_df = silver_df.filter(
        F.col("patient_id").isNotNull() & F.col("timestamp").isNotNull()
    )
    
    # ─── SELECT FINAL COLUMNS ───
    final_columns = [
        "patient_id", "device_id", "timestamp", "measurement_type",
        "value", "unit", "test_code", "result_value",
        "reference_range_low", "reference_range_high", "collected_at",
        "is_abnormal", "rolling_mean", "rolling_std", "z_score",
        "is_outlier", "time_bucket", "_silver_processed_at",
        "_source_system", "_file_date", "_ingested_at", "_source_file", "_run_id"
    ]
    
    # Only select columns that exist in the dataframe
    available_columns = [c for c in final_columns if c in clean_df.columns]
    output_df = clean_df.select(*available_columns)
    
    # ─── MERGE INTO DELTA (Upsert) ───
    if DeltaTable.isDeltaTable(spark, f"spark-warehouse/{TARGET_TABLE}") or spark.catalog.tableExists(TARGET_TABLE):
        target_delta = DeltaTable.forName(spark, TARGET_TABLE)
        
        (
            target_delta.alias("target")
            .merge(
                output_df.alias("source"),
                """
                target.patient_id = source.patient_id
                AND target.timestamp = source.timestamp
                AND target.measurement_type = source.measurement_type
                AND target._source_system = source._source_system
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"Batch {batch_id}: MERGE completed into {TARGET_TABLE}")
    else:
        # First write - create table
        output_df.write.format("delta").mode("overwrite").partitionBy("measurement_type").saveAsTable(TARGET_TABLE)
        logger.info(f"Batch {batch_id}: Initial write to {TARGET_TABLE}")
    
    logger.info(f"Batch {batch_id}: ✅ Silver write complete: {output_df.count()} records")

# COMMAND ----------

# DBTITLE 1,Execute Streaming Pipeline with foreachBatch
try:
    stream_query = (
        raw_df
        .writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )
    
    # Wait for the stream to finish processing all available data
    stream_query.awaitTermination()
    logger.info("✅ Streaming pipeline completed successfully")

except Exception as e:
    logger.error(f"❌ Pipeline failed: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Post-Processing: OPTIMIZE with ZORDER
spark.sql(f"""
    OPTIMIZE {TARGET_TABLE}
    ZORDER BY (patient_id, timestamp)
""")

logger.info(f"✅ OPTIMIZE with ZORDER completed on {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Validate Output and Log Metrics
# Final record count
final_count = spark.table(TARGET_TABLE).filter(
    (F.col("_source_system") == SOURCE_SYSTEM) & 
    (F.col("_file_date") == FILE_DATE)
).count()

# Table history for audit
history_df = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE} LIMIT 5")
display(history_df)

# Summary metrics
print("=" * 60)
print(f"Pipeline: bronze_to_silver")
print(f"Source System: {SOURCE_SYSTEM}")
print(f"File Date: {FILE_DATE}")
print(f"Run ID: {RUN_ID}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records for this run: {final_count}")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Exit with Success Status
dbutils.notebook.exit(f"SUCCESS: {final_count} records written to {TARGET_TABLE}")
