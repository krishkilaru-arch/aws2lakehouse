"""
NAV (Net Asset Value) Calculations — Fund-level valuations.

Used by: nav_calculation_job.py, investor_reporting_job.py
"""
from pyspark.sql import DataFrame, functions as F
from decimal import Decimal


def calculate_fund_nav(
    positions_df: DataFrame,
    cash_df: DataFrame,
    accruals_df: DataFrame,
    shares_outstanding_df: DataFrame
) -> DataFrame:
    """
    Calculate Net Asset Value per share for each fund.
    
    NAV = (Sum of Position Market Values + Cash - Accrued Expenses) / Shares Outstanding
    """
    # Total market value per fund
    fund_mv = (
        positions_df
        .groupBy("fund_id")
        .agg(F.sum("market_value").alias("total_market_value"))
    )
    
    # Cash balances per fund
    fund_cash = (
        cash_df
        .groupBy("fund_id")
        .agg(F.sum("balance").alias("total_cash"))
    )
    
    # Accrued expenses per fund
    fund_accruals = (
        accruals_df
        .groupBy("fund_id")
        .agg(F.sum("accrued_amount").alias("total_accruals"))
    )
    
    # Calculate NAV
    nav = (
        fund_mv
        .join(fund_cash, "fund_id", "left")
        .join(fund_accruals, "fund_id", "left")
        .join(shares_outstanding_df, "fund_id", "left")
        .withColumn("gross_asset_value",
            F.coalesce(F.col("total_market_value"), F.lit(0)) +
            F.coalesce(F.col("total_cash"), F.lit(0)))
        .withColumn("net_asset_value",
            F.col("gross_asset_value") - F.coalesce(F.col("total_accruals"), F.lit(0)))
        .withColumn("nav_per_share",
            F.when(F.col("shares_outstanding") > 0,
                   F.col("net_asset_value") / F.col("shares_outstanding"))
            .otherwise(F.lit(0.0)))
        .withColumn("calculation_timestamp", F.current_timestamp())
    )
    return nav


def calculate_performance_attribution(
    positions_df: DataFrame,
    benchmark_df: DataFrame,
    prices_start: DataFrame,
    prices_end: DataFrame
) -> DataFrame:
    """Calculate performance attribution vs benchmark (Brinson model)."""
    # Simplified Brinson attribution
    return (
        positions_df
        .join(benchmark_df, "gics_sector")
        .withColumn("allocation_effect",
            (F.col("portfolio_weight") - F.col("benchmark_weight")) * F.col("benchmark_return"))
        .withColumn("selection_effect",
            F.col("benchmark_weight") * (F.col("portfolio_return") - F.col("benchmark_return")))
        .withColumn("interaction_effect",
            (F.col("portfolio_weight") - F.col("benchmark_weight")) *
            (F.col("portfolio_return") - F.col("benchmark_return")))
        .withColumn("total_active_return",
            F.col("allocation_effect") + F.col("selection_effect") + F.col("interaction_effect"))
    )
