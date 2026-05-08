# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Aggregation Pipeline
# MAGIC **Domain:** Finance | **Layer:** Gold | **Pattern:** Silver → Gold Aggregation
# MAGIC
# MAGIC Builds aggregated views, KPIs, and reporting tables from Silver layer.
# MAGIC Config-driven: aggregation specs are parameterized via widgets.

# COMMAND ----------

# Parameters
dbutils.widgets.text("CATALOG", "acme_prod", "Unity Catalog")
dbutils.widgets.text("SCHEMA", "finance_gold", "Target Schema")
dbutils.widgets.text("SOURCE_SCHEMA", "finance_silver", "Source Schema")
dbutils.widgets.text("SOURCE_NAME", "", "Source table name")
dbutils.widgets.text("CONFIG", "{}", "Aggregation config JSON")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
SOURCE_SCHEMA = dbutils.widgets.get("SOURCE_SCHEMA")
SOURCE_NAME = dbutils.widgets.get("SOURCE_NAME")
CONFIG = dbutils.widgets.get("CONFIG")

# COMMAND ----------

import json
import logging
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_aggregation")

# Parse config
config = json.loads(CONFIG)

source_table = f"{CATALOG}.{SOURCE_SCHEMA}.{SOURCE_NAME}"
target_base = f"{CATALOG}.{SCHEMA}"

logger.info(f"Gold aggregation: {source_table} → {target_base}")
print(f"Gold aggregation: {source_table} → {target_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Layer

# COMMAND ----------

# Read from Silver Delta table in Unity Catalog
df = spark.read.table(source_table)

record_count = df.count()
logger.info(f"Read {record_count} records from Silver table: {source_table}")
print(f"Read {record_count} records from Silver table: {source_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Config-Driven Aggregations

# COMMAND ----------

def build_agg_expression(metric: dict):
    """Build a Spark aggregation expression from a metric spec."""
    col = metric["column"]
    func = metric["function"]
    alias = metric.get("alias", f"{func}_{col}")
    
    agg_map = {
        "sum": F.sum,
        "avg": F.avg,
        "count": F.count,
        "count_distinct": F.countDistinct,
        "max": F.max,
        "min": F.min,
    }
    
    if func not in agg_map:
        raise ValueError(f"Unsupported aggregation function: {func}. Supported: {list(agg_map.keys())}")
    
    return agg_map[func](col).alias(alias)

# COMMAND ----------

# Process each aggregation spec
agg_specs = config.get("gold_aggregations", [])

if not agg_specs:
    logger.warning("No aggregation specs found in config. Nothing to process.")
    print("⚠️ No aggregation specs found in config.")

results_summary = []

for spec in agg_specs:
    agg_name = spec["name"]
    group_cols = spec["group_by"]
    target_table = f"{target_base}.{agg_name}"
    
    logger.info(f"Processing aggregation: {agg_name}")
    print(f"\n--- Processing: {agg_name} ---")
    print(f"  Group by: {group_cols}")
    print(f"  Target: {target_table}")
    
    try:
        # Build aggregation expressions
        agg_exprs = [build_agg_expression(metric) for metric in spec["metrics"]]
        
        # Execute aggregation
        agg_df = df.groupBy(*group_cols).agg(*agg_exprs)
        
        # Apply post-aggregation filters (HAVING equivalent)
        if "having" in spec:
            having_clause = spec["having"]
            agg_df = agg_df.filter(having_clause)
            print(f"  Applied HAVING filter: {having_clause}")
        
        # Add audit columns
        agg_df = (
            agg_df
            .withColumn("_gold_created_at", F.current_timestamp())
            .withColumn("_source_table", F.lit(source_table))
            .withColumn("_aggregation_name", F.lit(agg_name))
        )
        
        # Write as Delta table with overwrite (gold aggregations are full rebuilds)
        (
            agg_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        
        row_count = agg_df.count()
        results_summary.append({"aggregation": agg_name, "rows": row_count, "status": "SUCCESS"})
        logger.info(f"  Gold/{agg_name}: {row_count} rows written to {target_table}")
        print(f"  ✅ Gold/{agg_name}: {row_count} rows written")
        
    except Exception as e:
        error_msg = str(e)
        results_summary.append({"aggregation": agg_name, "rows": 0, "status": f"FAILED: {error_msg}"})
        logger.error(f"  ❌ Failed to process {agg_name}: {error_msg}")
        print(f"  ❌ FAILED: {error_msg}")
        # Continue processing remaining aggregations
        continue

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Tables

# COMMAND ----------

# Optimize each successfully written gold table with ZORDER on group_by columns
for spec in agg_specs:
    agg_name = spec["name"]
    target_table = f"{target_base}.{agg_name}"
    group_cols = spec["group_by"]
    
    # Check if this aggregation succeeded
    result = next((r for r in results_summary if r["aggregation"] == agg_name), None)
    if result and result["status"] == "SUCCESS" and group_cols:
        try:
            zorder_cols = ", ".join(group_cols[:4])  # ZORDER supports up to 4 columns effectively
            spark.sql(f"OPTIMIZE {target_table} ZORDER BY ({zorder_cols})")
            logger.info(f"  Optimized {target_table} with ZORDER BY ({zorder_cols})")
            print(f"  🔧 Optimized {target_table} ZORDER BY ({zorder_cols})")
        except Exception as e:
            logger.warning(f"  OPTIMIZE failed for {target_table}: {e}")
            print(f"  ⚠️ OPTIMIZE skipped for {target_table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

# Print final summary
print("\n" + "=" * 60)
print("GOLD AGGREGATION PIPELINE SUMMARY")
print("=" * 60)
print(f"Source: {source_table}")
print(f"Source records: {record_count}")
print(f"Aggregations processed: {len(agg_specs)}")
print("-" * 60)

for result in results_summary:
    status_icon = "✅" if result["status"] == "SUCCESS" else "❌"
    print(f"  {status_icon} {result['aggregation']}: {result['rows']} rows | {result['status']}")

print("=" * 60)

# Fail the notebook if any aggregation failed
failed = [r for r in results_summary if r["status"] != "SUCCESS"]
if failed:
    failed_names = [r["aggregation"] for r in failed]
    raise RuntimeError(f"Gold aggregation pipeline completed with failures: {failed_names}")

print("✅ Gold layer complete")

# COMMAND ----------

# Exit with success metadata for orchestration
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "source_table": source_table,
    "aggregations_processed": len(agg_specs),
    "results": results_summary
}))
