# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformation Pipeline
# MAGIC **Domain:** Finance | **Layer:** Silver
# MAGIC
# MAGIC Applies business transforms defined in configuration:
# MAGIC - Column type casting
# MAGIC - Business logic (derived columns)
# MAGIC - Deduplication
# MAGIC - Joins with reference data
# MAGIC - PII masking
# MAGIC - Filtering & renaming

# COMMAND ----------

# Parameters
dbutils.widgets.text("CATALOG", "acme_prod", "Unity Catalog")
dbutils.widgets.text("SCHEMA", "finance_silver", "Target Schema")
dbutils.widgets.text("TABLE", "silver_transformation", "Target Table")
dbutils.widgets.text("SOURCE_CATALOG", "acme_prod", "Source Catalog")
dbutils.widgets.text("SOURCE_SCHEMA", "finance_bronze", "Source Schema")
dbutils.widgets.text("SOURCE_TABLE", "bronze_ingestion", "Source Table")
dbutils.widgets.text("CONFIG_JSON", "{}", "Transform Configuration JSON")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
TABLE = dbutils.widgets.get("TABLE")
SOURCE_CATALOG = dbutils.widgets.get("SOURCE_CATALOG")
SOURCE_SCHEMA = dbutils.widgets.get("SOURCE_SCHEMA")
SOURCE_TABLE = dbutils.widgets.get("SOURCE_TABLE")
CONFIG_JSON = dbutils.widgets.get("CONFIG_JSON")

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"
SOURCE_TABLE_FQN = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"

print(f"Silver transform: {SOURCE_TABLE_FQN} → {TARGET_TABLE}")

# COMMAND ----------

import json
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# COMMAND ----------

# Parse transform configuration
# Config can be passed as a widget parameter or retrieved from a config table
if CONFIG_JSON and CONFIG_JSON != "{}":
    config = json.loads(CONFIG_JSON)
else:
    # Fallback: read config from a Unity Catalog config table
    try:
        config_df = spark.sql(f"""
            SELECT config_json 
            FROM {CATALOG}.finance_config.pipeline_configs 
            WHERE pipeline_name = 'silver_transformation' 
            AND is_active = true
            ORDER BY version DESC
            LIMIT 1
        """)
        if config_df.count() > 0:
            config = json.loads(config_df.collect()[0]["config_json"])
        else:
            config = {}
            print("⚠️ No active config found, proceeding with empty transforms")
    except Exception as e:
        print(f"⚠️ Config table not available, using empty config: {e}")
        config = {}

print(f"Config loaded with {len(config.get('silver_transforms', []))} transforms")
print(f"Config version: {config.get('version', '1.0')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Layer (Source)

# COMMAND ----------

# Read from Bronze Delta table in Unity Catalog
df = spark.read.table(SOURCE_TABLE_FQN)

initial_count = df.count()
print(f"📥 Read {initial_count:,} rows from Bronze: {SOURCE_TABLE_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Silver Transforms from Configuration

# COMMAND ----------

def apply_transforms(df, config):
    """
    Apply a sequence of transforms defined in the configuration.
    Supports: cast, derive, dedup, filter, mask, rename
    """
    transforms = config.get("silver_transforms", [])
    
    for i, transform in enumerate(transforms):
        t_type = transform["type"]
        print(f"  [{i+1}/{len(transforms)}] Applying transform: {t_type}")
        
        if t_type == "cast":
            for col_name, dtype in transform["columns"].items():
                if col_name in df.columns:
                    df = df.withColumn(col_name, F.col(col_name).cast(dtype))
                else:
                    print(f"    ⚠️ Column '{col_name}' not found, skipping cast")
        
        elif t_type == "derive":
            for col_name, expr in transform["expressions"].items():
                df = df.withColumn(col_name, F.expr(expr))
        
        elif t_type == "dedup":
            key_cols = transform["key_columns"]
            order_col = transform["order_by"]
            window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
            before_count = df.count()
            df = (
                df.withColumn("_rn", F.row_number().over(window))
                .filter("_rn = 1")
                .drop("_rn")
            )
            after_count = df.count()
            print(f"    Dedup removed {before_count - after_count:,} duplicates")
        
        elif t_type == "filter":
            before_count = df.count()
            df = df.filter(transform["condition"])
            after_count = df.count()
            print(f"    Filter removed {before_count - after_count:,} rows")
        
        elif t_type == "mask":
            for col_name in transform["columns"]:
                if col_name in df.columns:
                    strategy = transform.get("strategy", "hash")
                    if strategy == "hash":
                        df = df.withColumn(col_name, F.sha2(F.col(col_name).cast("string"), 256))
                    elif strategy == "redact":
                        df = df.withColumn(col_name, F.lit("[MASKED]"))
                    print(f"    Masked column '{col_name}' with strategy: {strategy}")
                else:
                    print(f"    ⚠️ Column '{col_name}' not found, skipping mask")
        
        elif t_type == "rename":
            for old_name, new_name in transform["mapping"].items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)
                else:
                    print(f"    ⚠️ Column '{old_name}' not found, skipping rename")
        
        else:
            print(f"    ⚠️ Unknown transform type: {t_type}, skipping")
    
    return df

# Apply all transforms
print("🔄 Applying silver transforms...")
df = apply_transforms(df, config)
print("✅ All transforms applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Silver Metadata & Audit Columns

# COMMAND ----------

# Add silver metadata and audit columns
df = (
    df
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_silver_version", F.lit(config.get("version", "1.0")))
    .withColumn("_ingested_at", F.current_timestamp())
)

# Preserve _source_file if it exists from bronze layer, otherwise add placeholder
if "_source_file" not in df.columns:
    df = df.withColumn("_source_file", F.lit(SOURCE_TABLE_FQN))

row_count = df.count()
print(f"📊 Final row count after transforms: {row_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table (Merge/Upsert)

# COMMAND ----------

# Determine merge keys from config (dedup keys or explicit merge keys)
merge_keys = config.get("merge_keys", None)
if not merge_keys:
    # Try to extract from dedup transform
    for t in config.get("silver_transforms", []):
        if t["type"] == "dedup":
            merge_keys = t["key_columns"]
            break

# Determine partition columns
partition_cols = config.get("silver_partition_by", [])
# Filter out partition columns that don't exist in the dataframe
partition_cols = [c for c in partition_cols if c in df.columns]

# COMMAND ----------

# Write using MERGE if merge keys are defined, otherwise overwrite
if merge_keys and spark.catalog.tableExists(TARGET_TABLE):
    print(f"🔀 Performing MERGE on keys: {merge_keys}")
    
    target_delta = DeltaTable.forName(spark, TARGET_TABLE)
    
    # Build merge condition
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    
    # Perform merge (upsert)
    merge_result = (
        target_delta.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print(f"✅ MERGE complete into {TARGET_TABLE}")
    
else:
    print(f"📝 Writing (overwrite) to {TARGET_TABLE}")
    
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
        print(f"   Partitioned by: {partition_cols}")
    
    writer.saveAsTable(TARGET_TABLE)
    
    print(f"✅ Write complete to {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Write Optimization

# COMMAND ----------

# Determine ZORDER columns from config or use merge keys
zorder_cols = config.get("zorder_columns", merge_keys or [])

if zorder_cols:
    # Filter to columns that exist in the table
    table_columns = [f.name for f in spark.table(TARGET_TABLE).schema.fields]
    zorder_cols = [c for c in zorder_cols if c in table_columns]

if zorder_cols:
    zorder_clause = ", ".join(zorder_cols)
    print(f"⚡ Running OPTIMIZE with ZORDER BY ({zorder_clause})")
    spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY ({zorder_clause})")
else:
    print(f"⚡ Running OPTIMIZE (no ZORDER columns configured)")
    spark.sql(f"OPTIMIZE {TARGET_TABLE}")

print("✅ Optimization complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation & Summary

# COMMAND ----------

# Final validation
final_count = spark.table(TARGET_TABLE).count()
latest_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE} LIMIT 1").select("version").collect()[0][0]

summary = {
    "pipeline": "silver_transformation",
    "domain": "finance",
    "source_table": SOURCE_TABLE_FQN,
    "target_table": TARGET_TABLE,
    "input_rows": initial_count,
    "output_rows": row_count,
    "table_total_rows": final_count,
    "delta_version": latest_version,
    "config_version": config.get("version", "1.0"),
    "transforms_applied": len(config.get("silver_transforms", [])),
    "status": "SUCCESS"
}

print("\n" + "=" * 60)
print("📋 SILVER TRANSFORMATION SUMMARY")
print("=" * 60)
for k, v in summary.items():
    print(f"  {k}: {v}")
print("=" * 60)
print(f"\n✅ Silver transformation complete: {row_count:,} rows → {TARGET_TABLE}")

# COMMAND ----------

# Exit with success status for orchestration
dbutils.notebook.exit(json.dumps(summary))
