# Databricks notebook source
# MAGIC %md
# MAGIC # Risk Aggregation — Silver Layer
# MAGIC **Domain:** risk | **Classification:** MNPI
# MAGIC **Library:** acme_datalib v3.0.0

# COMMAND ----------

# MAGIC %pip install /Volumes/production/libraries/acme_datalib-3.0.0-py3-none-any.whl --quiet

# COMMAND ----------

from acme_datalib.transformations.positions import calculate_pnl, aggregate_by_sector, calculate_concentration_risk
from acme_datalib.transformations.market_data import detect_stale_prices
from acme_datalib.quality.validators import DataValidator
from pyspark.sql import functions as F
import os

CATALOG = os.getenv("CATALOG", "production")

# COMMAND ----------

# Read from Bronze Delta tables (not S3!)
positions_df = spark.table(f"{CATALOG}.positions_bronze.daily_positions")
market_data_df = spark.table(f"{CATALOG}.market_data_bronze.bloomberg_pricing")
reference_df = spark.table(f"{CATALOG}.reference.instruments")

# COMMAND ----------

# Same library functions — exact same business logic!
pnl_df = calculate_pnl(positions_df, market_data_df)
sector_df = aggregate_by_sector(pnl_df, reference_df)
concentration_df = calculate_concentration_risk(pnl_df, threshold_pct=10.0)

# COMMAND ----------

# Write to Silver
pnl_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.risk_silver.position_pnl")
sector_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.risk_silver.sector_exposure")
concentration_df.filter(F.col("concentration_breach")).write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.risk_silver.concentration_breaches")
print("✅ Risk aggregation complete")

