from typing import Dict

"""

Medallion Architecture Templates — Bronze, Silver, Gold layer patterns.

Provides ready-to-use notebook templates for each layer:
- Bronze: Raw ingestion with metadata, schema evolution, rescue column
- Silver: Cleansed, deduplicated, typed, joined data
- Gold: Business-level aggregates, KPIs, serving tables
"""


BRONZE_TEMPLATE = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: {table_name}
# MAGIC 
# MAGIC **Source:** {source_description}  
# MAGIC **Pattern:** {ingestion_pattern}  
# MAGIC **Schedule:** {schedule}

# COMMAND ----------

# Parameters
dbutils.widgets.text("catalog", "{catalog}")
dbutils.widgets.text("schema", "{schema}_bronze")
dbutils.widgets.text("table", "{table_name}")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
target_table = f"{{catalog}}.{{schema}}.{{table}}"

# COMMAND ----------

from pyspark.sql import functions as F

# Auto Loader ingestion with schema evolution
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "{file_format}")
    .option("cloudFiles.schemaLocation", f"/checkpoints/{{target_table}}/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load("{source_path}")
)

# Add ingestion metadata
df = (df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingestion_batch_id", F.lit(spark.conf.get("spark.databricks.job.runId", "interactive")))
)

# COMMAND ----------

# Write to Bronze (append-only, preserve raw data)
(df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", f"/checkpoints/{{target_table}}")
    .trigger(availableNow=True)
    .toTable(target_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
display(spark.sql(f"SELECT COUNT(*) as row_count, MAX(_ingested_at) as latest_ingestion FROM {{target_table}}"))
'''


SILVER_TEMPLATE = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: {table_name}
# MAGIC
# MAGIC **Source:** Bronze.{source_table}  
# MAGIC **Transformations:** Dedup, type casting, null handling, business rules  
# MAGIC **SCD Type:** {scd_type}

# COMMAND ----------

# Parameters
dbutils.widgets.text("catalog", "{catalog}")
dbutils.widgets.text("source_schema", "{schema}_bronze")
dbutils.widgets.text("target_schema", "{schema}_silver")
dbutils.widgets.text("table", "{table_name}")

catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
target_schema = dbutils.widgets.get("target_schema")
table = dbutils.widgets.get("table")

source_table = f"{{catalog}}.{{source_schema}}.{{table}}"
target_table = f"{{catalog}}.{{target_schema}}.{{table}}"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Read from Bronze
bronze_df = spark.readStream.table(source_table)

# COMMAND ----------

# Data Quality: Remove nulls on required fields, cast types
cleaned_df = (bronze_df
    .filter(F.col("{primary_key}").isNotNull())
    .dropDuplicates(["{primary_key}"])
    .withColumn("_cleaned_at", F.current_timestamp())
    .withColumn("_is_valid", F.lit(True))
)

# COMMAND ----------

# Type casting and standardization
silver_df = (cleaned_df
    # Add business logic transformations here
    .withColumn("_updated_at", F.current_timestamp())
)

# COMMAND ----------

# Write to Silver with merge (upsert)
def upsert_to_silver(batch_df, batch_id):
    if not spark.catalog.tableExists(target_table):
        batch_df.write.format("delta").saveAsTable(target_table)
        return
    
    target = DeltaTable.forName(spark, target_table)
    (target.alias("t")
        .merge(batch_df.alias("s"), "t.{primary_key} = s.{primary_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

(silver_df.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", f"/checkpoints/{{target_table}}")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics
display(spark.sql(f"SELECT COUNT(*) as total, SUM(CASE WHEN _is_valid THEN 1 ELSE 0 END) as valid FROM {{target_table}}"))
'''


GOLD_TEMPLATE = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: {table_name}
# MAGIC
# MAGIC **Source:** Silver tables  
# MAGIC **Purpose:** {business_purpose}  
# MAGIC **Consumers:** {consumers}

# COMMAND ----------

# Parameters
dbutils.widgets.text("catalog", "{catalog}")
dbutils.widgets.text("schema", "{schema}_gold")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
target_table = f"{{catalog}}.{{schema}}.{table_name}"

# COMMAND ----------

from pyspark.sql import functions as F

# Read from Silver layer(s)
{silver_reads}

# COMMAND ----------

# Business aggregations and KPIs
gold_df = (
    {aggregation_logic}
)

# COMMAND ----------

# Write to Gold (overwrite for aggregates, or merge for slowly-changing)
(gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

# Optimize for query performance
spark.sql(f"OPTIMIZE {{target_table}} ZORDER BY ({zorder_columns})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Summary
display(spark.sql(f"SELECT COUNT(*) as rows, CURRENT_TIMESTAMP() as refreshed_at FROM {{target_table}}"))
'''


def generate_medallion_notebooks(
    table_name: str,
    catalog: str = "production",
    schema: str = "lending",
    source_path: str = "/Volumes/production/raw/",
    primary_key: str = "id",
    file_format: str = "json"
) -> Dict[str, str]:
    """Generate Bronze, Silver, Gold notebooks for a table."""
    
    bronze = BRONZE_TEMPLATE.format(
        table_name=table_name,
        catalog=catalog,
        schema=schema,
        source_path=source_path,
        file_format=file_format,
        source_description=f"Auto Loader from {source_path}",
        ingestion_pattern="Auto Loader with schema evolution",
        schedule="Daily at 6 AM UTC"
    )
    
    silver = SILVER_TEMPLATE.format(
        table_name=table_name,
        catalog=catalog,
        schema=schema,
        source_table=table_name,
        primary_key=primary_key,
        scd_type="Type 1 (Overwrite)"
    )
    
    gold = GOLD_TEMPLATE.format(
        table_name=f"{table_name}_summary",
        catalog=catalog,
        schema=schema,
        business_purpose="Business aggregations and KPIs",
        consumers="Analytics dashboards, Reporting",
        silver_reads=f'silver_df = spark.table(f"{{catalog}}.{schema}_silver.{table_name}")',
        aggregation_logic=f'silver_df.groupBy("date").agg(F.count("*").alias("total_count"))',
        zorder_columns="date"
    )
    
    return {
        f"bronze_{table_name}.py": bronze,
        f"silver_{table_name}.py": silver,
        f"gold_{table_name}_summary.py": gold
    }
