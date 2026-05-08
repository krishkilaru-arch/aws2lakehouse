"""
Position Transformations — Calculate P&L, exposure, risk metrics.

Used by: position_reconciliation_job.py, risk_aggregation_job.py
"""
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType
from typing import Dict


POSITION_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("instrument_id", StringType(), False),
    StructField("quantity", LongType(), False),
    StructField("avg_cost", DoubleType(), False),
    StructField("market_value", DoubleType(), True),
    StructField("unrealized_pnl", DoubleType(), True),
    StructField("realized_pnl", DoubleType(), True),
    StructField("position_date", DateType(), False),
])


def calculate_pnl(positions_df: DataFrame, prices_df: DataFrame) -> DataFrame:
    """
    Calculate unrealized P&L by joining positions with current market prices.
    
    P&L = (current_price - avg_cost) * quantity
    """
    latest_prices = (
        prices_df
        .filter(F.col("price_type") == "last")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("instrument_id").orderBy(F.desc("timestamp"))))
        .filter(F.col("rn") == 1)
        .select("instrument_id", F.col("price").alias("current_price"), "currency")
    )
    
    return (
        positions_df
        .join(latest_prices, "instrument_id", "left")
        .withColumn("market_value", F.col("current_price") * F.col("quantity"))
        .withColumn("unrealized_pnl", 
            (F.col("current_price") - F.col("avg_cost")) * F.col("quantity"))
        .withColumn("pnl_pct", 
            F.when(F.col("avg_cost") != 0,
                   (F.col("current_price") - F.col("avg_cost")) / F.col("avg_cost") * 100)
            .otherwise(F.lit(0.0)))
    )


def aggregate_by_sector(positions_df: DataFrame, reference_df: DataFrame) -> DataFrame:
    """Aggregate positions by GICS sector for risk reporting."""
    return (
        positions_df
        .join(reference_df.select("instrument_id", "gics_sector", "gics_industry"), 
              "instrument_id", "left")
        .groupBy("account_id", "gics_sector")
        .agg(
            F.sum("market_value").alias("total_market_value"),
            F.sum("unrealized_pnl").alias("total_unrealized_pnl"),
            F.count("instrument_id").alias("position_count"),
            F.collect_set("instrument_id").alias("instruments"),
        )
    )


def calculate_concentration_risk(positions_df: DataFrame, threshold_pct: float = 10.0) -> DataFrame:
    """Flag positions exceeding concentration threshold."""
    total_window = Window.partitionBy("account_id")
    return (
        positions_df
        .withColumn("portfolio_total", F.sum("market_value").over(total_window))
        .withColumn("concentration_pct", 
            F.col("market_value") / F.col("portfolio_total") * 100)
        .withColumn("concentration_breach",
            F.col("concentration_pct") > threshold_pct)
    )


def reconcile_with_custodian(
    internal_positions: DataFrame, 
    custodian_positions: DataFrame,
    tolerance: float = 0.01
) -> DataFrame:
    """
    Reconcile internal positions against custodian (e.g., State Street, BNY Mellon).
    Returns breaks (mismatches) that need investigation.
    """
    recon = (
        internal_positions.alias("int")
        .join(
            custodian_positions.alias("cust"),
            (F.col("int.account_id") == F.col("cust.account_id")) &
            (F.col("int.instrument_id") == F.col("cust.instrument_id")) &
            (F.col("int.position_date") == F.col("cust.position_date")),
            "full_outer"
        )
        .withColumn("qty_diff", 
            F.coalesce(F.col("int.quantity"), F.lit(0)) - 
            F.coalesce(F.col("cust.quantity"), F.lit(0)))
        .withColumn("mv_diff",
            F.abs(F.coalesce(F.col("int.market_value"), F.lit(0)) -
                  F.coalesce(F.col("cust.market_value"), F.lit(0))))
        .withColumn("break_type",
            F.when(F.col("int.instrument_id").isNull(), F.lit("MISSING_INTERNAL"))
            .when(F.col("cust.instrument_id").isNull(), F.lit("MISSING_CUSTODIAN"))
            .when(F.col("qty_diff") != 0, F.lit("QUANTITY_BREAK"))
            .when(F.col("mv_diff") > tolerance, F.lit("VALUE_BREAK"))
            .otherwise(F.lit("MATCHED")))
    )
    return recon
