# Tests for customer_360
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.customer_bronze.customer_360")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.customer_bronze.customer_360").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_customer_360_id(spark):
    df = spark.table("acme_prod.customer_bronze.customer_360")
    total = df.count()
    passing = df.filter("customer_360_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0