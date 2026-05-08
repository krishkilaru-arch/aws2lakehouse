# Tests for market_data_feed
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.risk_bronze.market_data_feed")

def test_no_null_keys(spark):
    df = spark.table("production.risk_bronze.market_data_feed")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.risk_bronze.market_data_feed").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=2)

def test_dq_valid_market_data_feed_id(spark):
    df = spark.table("production.risk_bronze.market_data_feed")
    total = df.count()
    passing = df.filter("market_data_feed_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0