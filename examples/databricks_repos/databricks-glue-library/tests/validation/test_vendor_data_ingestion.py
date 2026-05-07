# Tests for vendor_data_ingestion
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.finance_bronze.vendor_data_ingestion")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.finance_bronze.vendor_data_ingestion").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_vendor_data_ingestion_id(spark):
    df = spark.table("acme_prod.finance_bronze.vendor_data_ingestion")
    total = df.count()
    passing = df.filter("vendor_data_ingestion_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0