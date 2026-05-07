# Tests for nav_calculation_job
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.finance_bronze.nav_calculation_job")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.finance_bronze.nav_calculation_job").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_nav_calculation_job_id(spark):
    df = spark.table("acme_prod.finance_bronze.nav_calculation_job")
    total = df.count()
    passing = df.filter("nav_calculation_job_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0