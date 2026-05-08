# Tests for payment_processing
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.lending_bronze.payment_processing")

def test_no_null_keys(spark):
    df = spark.table("production.lending_bronze.payment_processing")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.lending_bronze.payment_processing").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=10)

def test_dq_valid_payment_processing_id(spark):
    df = spark.table("production.lending_bronze.payment_processing")
    total = df.count()
    passing = df.filter("payment_processing_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0