"""Unit tests for market data transformations."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from acme_datalib.transformations.market_data import normalize_bloomberg_feed, detect_stale_prices


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_normalize_bloomberg_feed(spark):
    """Test Bloomberg feed normalization."""
    data = [
        ("AAPL US", "Equity", "PX_LAST", "150.25", "2024-01-15 10:30:00.000", "ACTIVE", "US", "USD"),
        ("MSFT US", "Equity", "PX_BID", "380.10", "2024-01-15 10:30:00.000", "ACTIVE", "US", "USD"),
        ("DEAD US", "Equity", "PX_LAST", "0.00", "2024-01-15 10:30:00.000", "INACTIVE", "US", "USD"),
    ]
    df = spark.createDataFrame(data, ["TICKER", "YELLOW_KEY", "FIELD", "VALUE", "DATETIME", "STATUS", "EXCH_CODE", "CRNCY"])
    
    result = normalize_bloomberg_feed(df)
    
    # Should filter out INACTIVE
    assert result.count() == 2
    # Should have standard columns
    assert "instrument_id" in result.columns
    assert "price_type" in result.columns
    # Price type should be normalized
    assert result.filter(F.col("price_type") == "last").count() == 1
    assert result.filter(F.col("price_type") == "bid").count() == 1


def test_detect_stale_prices(spark):
    """Test stale price detection."""
    from datetime import datetime, timedelta
    
    now = datetime.now()
    data = [
        ("AAPL", now - timedelta(minutes=5), 150.0),  # Fresh
        ("MSFT", now - timedelta(minutes=60), 380.0),  # Stale
    ]
    df = spark.createDataFrame(data, ["instrument_id", "timestamp", "price"])
    
    result = detect_stale_prices(df, staleness_threshold_minutes=15)
    stale = result.filter(F.col("is_stale")).collect()
    
    assert len(stale) == 1
    assert stale[0]["instrument_id"] == "MSFT"
