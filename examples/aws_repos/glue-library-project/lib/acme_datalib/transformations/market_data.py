"""
Market Data Transformations — normalize feeds from Bloomberg, Reuters, ICE.

Used by: market_data_ingest_job.py, eod_pricing_job.py, risk_calc_job.py
"""
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


# Standard schema for normalized market data
MARKET_DATA_SCHEMA = StructType([
    StructField("instrument_id", StringType(), False),
    StructField("ticker", StringType(), False),
    StructField("exchange", StringType(), True),
    StructField("price_type", StringType(), False),  # bid, ask, mid, last, close
    StructField("price", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("source", StringType(), False),  # bloomberg, reuters, ice
    StructField("quality_flag", StringType(), True),
])


def normalize_bloomberg_feed(df: DataFrame) -> DataFrame:
    """
    Normalize Bloomberg real-time feed into standard market data schema.
    
    Bloomberg delivers: TICKER|FIELD|VALUE|DATETIME|STATUS
    We normalize to: instrument_id, ticker, exchange, price_type, price, currency, timestamp, source
    """
    logger.info(f"Normalizing Bloomberg feed: {df.count()} records")
    
    return (
        df
        .filter(F.col("STATUS") == "ACTIVE")
        .withColumn("instrument_id", 
            F.concat(F.col("TICKER"), F.lit("_"), F.col("YELLOW_KEY")))
        .withColumn("ticker", F.col("TICKER"))
        .withColumn("exchange", 
            F.when(F.col("YELLOW_KEY") == "Equity", F.col("EXCH_CODE"))
            .when(F.col("YELLOW_KEY") == "Corp", F.lit("OTC"))
            .otherwise(F.col("YELLOW_KEY")))
        .withColumn("price_type",
            F.when(F.col("FIELD") == "PX_BID", F.lit("bid"))
            .when(F.col("FIELD") == "PX_ASK", F.lit("ask"))
            .when(F.col("FIELD") == "PX_MID", F.lit("mid"))
            .when(F.col("FIELD") == "PX_LAST", F.lit("last"))
            .when(F.col("FIELD") == "PX_CLOSE_1D", F.lit("close"))
            .otherwise(F.col("FIELD")))
        .withColumn("price", F.col("VALUE").cast(DoubleType()))
        .withColumn("currency", F.coalesce(F.col("CRNCY"), F.lit("USD")))
        .withColumn("timestamp", F.to_timestamp(F.col("DATETIME"), "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("source", F.lit("bloomberg"))
        .withColumn("quality_flag",
            F.when(F.col("price").isNull(), F.lit("MISSING_PRICE"))
            .when(F.col("price") <= 0, F.lit("NEGATIVE_PRICE"))
            .otherwise(F.lit("OK")))
        .select("instrument_id", "ticker", "exchange", "price_type", 
                "price", "currency", "timestamp", "source", "quality_flag")
    )


def normalize_reuters_feed(df: DataFrame) -> DataFrame:
    """Normalize Reuters/Refinitiv feed into standard schema."""
    return (
        df
        .filter(F.col("RIC").isNotNull())
        .withColumn("instrument_id", F.concat(F.col("RIC"), F.lit("_REUT")))
        .withColumn("ticker", F.regexp_extract(F.col("RIC"), r"^([A-Z]+)", 1))
        .withColumn("exchange", F.regexp_extract(F.col("RIC"), r"\.([A-Z]+)$", 1))
        .withColumn("price_type", F.lower(F.col("FIELD_NAME")))
        .withColumn("price", F.col("FIELD_VALUE").cast(DoubleType()))
        .withColumn("currency", F.coalesce(F.col("CCY"), F.lit("USD")))
        .withColumn("timestamp", F.to_timestamp(F.col("UPDATE_TIME")))
        .withColumn("source", F.lit("reuters"))
        .withColumn("quality_flag", F.lit("OK"))
        .select("instrument_id", "ticker", "exchange", "price_type",
                "price", "currency", "timestamp", "source", "quality_flag")
    )


def calculate_vwap(df: DataFrame, window_minutes: int = 60) -> DataFrame:
    """Calculate Volume-Weighted Average Price over rolling window."""
    window = Window.partitionBy("instrument_id").orderBy("timestamp").rangeBetween(
        -(window_minutes * 60), Window.currentRow
    )
    return (
        df
        .withColumn("vwap", 
            F.sum(F.col("price") * F.col("volume")).over(window) /
            F.sum("volume").over(window))
        .withColumn("vwap_window_minutes", F.lit(window_minutes))
    )


def detect_stale_prices(df: DataFrame, staleness_threshold_minutes: int = 15) -> DataFrame:
    """Flag instruments with stale prices (no update in N minutes)."""
    return (
        df
        .withColumn("minutes_since_update",
            (F.current_timestamp().cast("long") - F.col("timestamp").cast("long")) / 60)
        .withColumn("is_stale",
            F.col("minutes_since_update") > staleness_threshold_minutes)
    )


def apply_corporate_actions(prices_df: DataFrame, corp_actions_df: DataFrame) -> DataFrame:
    """Adjust historical prices for splits, dividends, mergers."""
    adjusted = (
        prices_df
        .join(corp_actions_df, 
              (prices_df.instrument_id == corp_actions_df.instrument_id) &
              (prices_df.timestamp < corp_actions_df.effective_date),
              "left")
        .withColumn("adjusted_price",
            F.when(F.col("action_type") == "SPLIT",
                   F.col("price") / F.col("adjustment_factor"))
            .when(F.col("action_type") == "DIVIDEND",
                   F.col("price") - F.col("dividend_amount"))
            .otherwise(F.col("price")))
        .drop(corp_actions_df.instrument_id)
    )
    return adjusted
