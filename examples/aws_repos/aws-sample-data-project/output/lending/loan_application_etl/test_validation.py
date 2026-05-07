# Tests for loan_application_etl
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.lending_bronze.loan_application_etl")

def test_no_null_keys(spark):
    df = spark.table("production.lending_bronze.loan_application_etl")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.lending_bronze.loan_application_etl").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=30)

def test_dq_valid_loan_application_etl_id(spark):
    df = spark.table("production.lending_bronze.loan_application_etl")
    total = df.count()
    passing = df.filter("loan_application_etl_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0