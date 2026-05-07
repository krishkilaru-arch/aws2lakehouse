# Tests for market_data_feed
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.risk_bronze.market_data_feed")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.risk_bronze.market_data_feed").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=5)

def test_dq_valid_market_data_feed_id(spark):
    df = spark.table("acme_prod.risk_bronze.market_data_feed")
    total = df.count()
    passing = df.filter("market_data_feed_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0