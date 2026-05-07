# Tests for trade_events_streaming
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.risk_bronze.trade_events_streaming")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.risk_bronze.trade_events_streaming").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=5)

def test_dq_valid_trade_events_streaming_id(spark):
    df = spark.table("acme_prod.risk_bronze.trade_events_streaming")
    total = df.count()
    passing = df.filter("trade_events_streaming_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0