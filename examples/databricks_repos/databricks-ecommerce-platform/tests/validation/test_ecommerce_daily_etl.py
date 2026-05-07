# Tests for ecommerce_daily_etl
def test_table_exists(spark):
    assert spark.catalog.tableExists("ecommerce_prod.analytics_bronze.ecommerce_daily_etl")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("ecommerce_prod.analytics_bronze.ecommerce_daily_etl").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_ecommerce_daily_etl_id(spark):
    df = spark.table("ecommerce_prod.analytics_bronze.ecommerce_daily_etl")
    total = df.count()
    passing = df.filter("ecommerce_daily_etl_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0