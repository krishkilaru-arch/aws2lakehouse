# Tests for bronze_to_silver
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.finance_bronze.bronze_to_silver")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.finance_bronze.bronze_to_silver").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_bronze_to_silver_id(spark):
    df = spark.table("acme_prod.finance_bronze.bronze_to_silver")
    total = df.count()
    passing = df.filter("bronze_to_silver_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0