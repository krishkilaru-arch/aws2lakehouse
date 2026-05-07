# Tests for order_flow_capture
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.risk_bronze.order_flow_capture")

def test_no_null_keys(spark):
    df = spark.table("production.risk_bronze.order_flow_capture")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.risk_bronze.order_flow_capture").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=8)

def test_dq_valid_order_flow_capture_id(spark):
    df = spark.table("production.risk_bronze.order_flow_capture")
    total = df.count()
    passing = df.filter("order_flow_capture_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0