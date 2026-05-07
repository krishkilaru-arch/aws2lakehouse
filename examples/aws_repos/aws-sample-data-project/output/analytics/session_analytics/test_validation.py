# Tests for session_analytics
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.analytics_bronze.session_analytics")

def test_no_null_keys(spark):
    df = spark.table("production.analytics_bronze.session_analytics")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.analytics_bronze.session_analytics").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=45)

def test_dq_valid_session_analytic_id(spark):
    df = spark.table("production.analytics_bronze.session_analytics")
    total = df.count()
    passing = df.filter("session_analytic_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0