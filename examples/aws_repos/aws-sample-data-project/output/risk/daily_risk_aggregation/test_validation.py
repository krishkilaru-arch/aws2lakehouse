# Tests for daily_risk_aggregation
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.risk_bronze.daily_risk_aggregation")

def test_no_null_keys(spark):
    df = spark.table("production.risk_bronze.daily_risk_aggregation")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.risk_bronze.daily_risk_aggregation").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=90)

def test_dq_valid_daily_risk_aggregation_id(spark):
    df = spark.table("production.risk_bronze.daily_risk_aggregation")
    total = df.count()
    passing = df.filter("daily_risk_aggregation_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0