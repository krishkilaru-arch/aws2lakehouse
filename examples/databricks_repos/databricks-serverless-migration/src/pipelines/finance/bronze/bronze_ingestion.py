# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion — Finance Domain
# MAGIC **Pipeline:** bronze_ingestion
# MAGIC **Layer:** Bronze (Raw → Delta)
# MAGIC **Target:** acme_prod.finance_bronze.bronze_ingestion
# MAGIC
# MAGIC Reads from Unity Catalog Volumes landing zone, applies schema evolution,
# MAGIC writes to Bronze Delta table. Handles: CSV, JSON, Parquet, XML.
# MAGIC Triggered by file arrival events via Databricks Workflows.

# COMMAND ----------

# Parameters — configurable via Databricks Workflows or interactive widgets
dbutils.widgets.text("CATALOG", "acme_prod", "Unity Catalog name")
dbutils.widgets.text("SCHEMA", "finance_bronze", "Target schema")
dbutils.widgets.text("TABLE", "bronze_ingestion", "Target table")
dbutils.widgets.text("SOURCE_VOLUME", "landing", "Source volume name")
dbutils.widgets.text("SOURCE_NAME", "", "Source system name (e.g., vendor_payments)")
dbutils.widgets.text("SOURCE_KEY", "", "Relative path/key within volume")
dbutils.widgets.text("CONFIG_JSON", '{"file_format":"csv","delimiter":","}', "Pipeline config JSON")

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# Resolve parameters
CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
TABLE = dbutils.widgets.get("TABLE")
SOURCE_VOLUME = dbutils.widgets.get("SOURCE_VOLUME")
SOURCE_NAME = dbutils.widgets.get("SOURCE_NAME")
SOURCE_KEY = dbutils.widgets.get("SOURCE_KEY")
CONFIG_JSON = dbutils.widgets.get("CONFIG_JSON")

# Parse config
config = json.loads(CONFIG_JSON)
file_format = config.get("file_format", "csv")
delimiter = config.get("delimiter", ",")

# Construct paths using Unity Catalog Volumes
source_path = f"/Volumes/{CATALOG}/{SCHEMA}/{SOURCE_VOLUME}/{SOURCE_KEY}"
target_table = f"{CATALOG}.{SCHEMA}.{TABLE}"
checkpoint_path = f"/Volumes/{CATALOG}/{SCHEMA}/{SOURCE_VOLUME}/_checkpoints/{TABLE}/{SOURCE_NAME}"

print(f"Bronze Ingestion Pipeline")
print(f"  Source:  {source_path}")
print(f"  Target:  {target_table}")
print(f"  Format:  {file_format}")
print(f"  Source:  {SOURCE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Target Schema and Table Exist

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Create target Delta table if not exists (schema will evolve via mergeSchema)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        _source_file STRING,
        _source_name STRING,
        _ingested_at TIMESTAMP,
        _file_format STRING,
        _ingestion_date DATE
    )
    USING DELTA
    PARTITIONED BY (_ingestion_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'bronze',
        'domain' = 'finance',
        'pipeline' = 'bronze_ingestion'
    )
""")

print(f"✅ Target table ensured: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Using Auto Loader (Structured Streaming with cloudFiles)
# MAGIC
# MAGIC Auto Loader provides:
# MAGIC - Exactly-once ingestion guarantees
# MAGIC - Schema evolution support
# MAGIC - Efficient file discovery (no re-processing)

# COMMAND ----------

def build_autoloader_reader(file_format: str, source_path: str, config: dict):
    """
    Build a Structured Streaming reader using Auto Loader (cloudFiles)
    based on the source file format.
    """
    base_options = {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaLocation": f"{checkpoint_path}/_schema",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
    }

    if file_format == "csv":
        base_options.update({
            "cloudFiles.format": "csv",
            "header": "true",
            "delimiter": config.get("delimiter", ","),
            "inferSchema": "true",
        })
    elif file_format == "json":
        base_options.update({
            "cloudFiles.format": "json",
            "multiLine": str(config.get("multi_line", False)).lower(),
        })
    elif file_format == "parquet":
        base_options.update({
            "cloudFiles.format": "parquet",
        })
    elif file_format == "xml":
        base_options.update({
            "cloudFiles.format": "xml",
            "rowTag": config.get("row_tag", "record"),
        })
    else:
        raise ValueError(f"Unsupported format: {file_format}")

    return (
        spark.readStream
        .format("cloudFiles")
        .options(**base_options)
        .load(source_path)
    )


stream_df = build_autoloader_reader(file_format, source_path, config)
print(f"✅ Auto Loader reader configured for format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations (Standardize + Audit Columns)

# COMMAND ----------

def transform_bronze(df):
    """
    Apply bronze-layer standardization:
    - Add audit/metadata columns
    - Rename columns to snake_case
    - Add ingestion date partition column
    """
    # Add metadata columns
    df = (
        df
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_name", F.lit(SOURCE_NAME))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_file_format", F.lit(file_format))
        .withColumn("_ingestion_date", F.current_date())
    )

    # Rename columns to snake_case (excluding metadata columns)
    for col_name in df.columns:
        new_name = col_name.lower().replace(" ", "_").replace("-", "_")
        if new_name != col_name:
            df = df.withColumnRenamed(col_name, new_name)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table (Streaming with availableNow)
# MAGIC
# MAGIC Using `trigger(availableNow=True)` for incremental batch processing:
# MAGIC - Processes all available files since last checkpoint
# MAGIC - Then stops (ideal for scheduled/event-driven workflows)

# COMMAND ----------

def write_bronze_microbatch(batch_df, batch_id):
    """
    foreachBatch handler: transform and write each micro-batch.
    Includes basic data quality logging.
    """
    if batch_df.isEmpty():
        print(f"  Batch {batch_id}: No new data to process.")
        return

    # Apply transformations
    transformed_df = transform_bronze(batch_df)

    row_count = transformed_df.count()
    col_count = len(transformed_df.columns)

    print(f"  Batch {batch_id}: Processing {row_count} rows, {col_count} columns")

    # Write to Delta with schema evolution enabled
    (
        transformed_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .saveAsTable(target_table)
    )

    print(f"  Batch {batch_id}: ✅ Written {row_count} rows to {target_table}")


# Execute streaming ingestion
write_stream = (
    stream_df
    .writeStream
    .foreachBatch(write_bronze_microbatch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

# Wait for completion (availableNow will auto-terminate)
write_stream.awaitTermination()
print("✅ Streaming ingestion complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Ingestion: Validate & Optimize

# COMMAND ----------

# Validate row count in target table for today's partition
today = str(F.current_date())
validation_df = spark.sql(f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT _source_file) as distinct_files,
        MIN(_ingested_at) as earliest_ingestion,
        MAX(_ingested_at) as latest_ingestion
    FROM {target_table}
    WHERE _ingestion_date = current_date()
      AND _source_name = '{SOURCE_NAME}'
""")

validation_df.show(truncate=False)

# COMMAND ----------

# Optimize table with ZORDER on commonly queried columns
# Run periodically (not every micro-batch) — controlled by workflow scheduling
spark.sql(f"""
    OPTIMIZE {target_table}
    WHERE _ingestion_date = current_date()
    ZORDER BY (_source_name, _ingested_at)
""")

print(f"✅ OPTIMIZE with ZORDER complete for {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & Exit

# COMMAND ----------

# Final summary for workflow observability
summary = spark.sql(f"""
    SELECT 
        _ingestion_date,
        _source_name,
        _file_format,
        COUNT(*) as row_count,
        COUNT(DISTINCT _source_file) as file_count,
        MAX(_ingested_at) as last_ingested
    FROM {target_table}
    WHERE _ingestion_date = current_date()
      AND _source_name = '{SOURCE_NAME}'
    GROUP BY _ingestion_date, _source_name, _file_format
""")

summary.show(truncate=False)

# Set exit value for downstream workflow steps
row_count_total = summary.select(F.sum("row_count")).collect()[0][0] or 0
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "table": target_table,
    "source_name": SOURCE_NAME,
    "rows_ingested": int(row_count_total),
    "format": file_format
}))
