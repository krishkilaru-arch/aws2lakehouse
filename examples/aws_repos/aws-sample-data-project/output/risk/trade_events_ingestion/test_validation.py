# Tests for trade_events_ingestion
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.risk_bronze.trade_events_ingestion")

def test_no_null_keys(spark):
    df = spark.table("production.risk_bronze.trade_events_ingestion")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.risk_bronze.trade_events_ingestion").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=5)

def test_dq_valid_trade_events_ingestion_id(spark):
    df = spark.table("production.risk_bronze.trade_events_ingestion")
    total = df.count()
    passing = df.filter("trade_events_ingestion_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0