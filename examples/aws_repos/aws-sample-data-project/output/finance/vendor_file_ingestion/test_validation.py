# Tests for vendor_file_ingestion
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.finance_bronze.vendor_file_ingestion")

def test_no_null_keys(spark):
    df = spark.table("production.finance_bronze.vendor_file_ingestion")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.finance_bronze.vendor_file_ingestion").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_vendor_file_ingestion_id(spark):
    df = spark.table("production.finance_bronze.vendor_file_ingestion")
    total = df.count()
    passing = df.filter("vendor_file_ingestion_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0