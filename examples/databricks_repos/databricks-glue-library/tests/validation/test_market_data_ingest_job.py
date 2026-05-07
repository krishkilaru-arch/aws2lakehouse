# Tests for market_data_ingest_job
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.finance_bronze.market_data_ingest_job")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.finance_bronze.market_data_ingest_job").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_market_data_ingest_job_id(spark):
    df = spark.table("acme_prod.finance_bronze.market_data_ingest_job")
    total = df.count()
    passing = df.filter("market_data_ingest_job_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0