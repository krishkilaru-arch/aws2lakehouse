# Databricks notebook source
# MAGIC %md
# MAGIC # Position Snapshot — Bronze Layer
# MAGIC **Domain:** positions | **Owner:** risk-engineering@acmecapital.com
# MAGIC **Library:** acme_datalib v3.0.0

# COMMAND ----------

# MAGIC %pip install /Volumes/production/libraries/acme_datalib-3.0.0-py3-none-any.whl --quiet

# COMMAND ----------

from acme_datalib.connectors.database import DatabaseConnector
from acme_datalib.transformations.positions import reconcile_with_custodian
from acme_datalib.quality.validators import DataValidator
from pyspark.sql import functions as F
import os

CATALOG = os.getenv("CATALOG", "production")
TARGET_TABLE = f"{CATALOG}.positions_bronze.daily_positions"

# COMMAND ----------

# Extract from OMS (PostgreSQL via JDBC + secrets)
oms = DatabaseConnector(spark, secret_scope="oms-postgres")
positions_df = oms.read_table("positions", partition_column="account_id", num_partitions=20)
positions_df = positions_df.withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

# Validate using shared library
validator = DataValidator(positions_df, "position_snapshot")
validator.check_not_null(["account_id", "instrument_id", "quantity", "position_date"])
validator.check_range("quantity", min_val=-1_000_000_000, max_val=1_000_000_000)
validator.validate(fail_on_error=True)

# COMMAND ----------

# Write to Delta (MERGE for idempotency)
positions_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
print(f"✅ Position snapshot → {TARGET_TABLE} ({positions_df.count()} rows)")

