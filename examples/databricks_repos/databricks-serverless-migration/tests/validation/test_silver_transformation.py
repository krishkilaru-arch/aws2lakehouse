# Tests for silver_transformation
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.finance_bronze.silver_transformation")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.finance_bronze.silver_transformation").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_silver_transformation_id(spark):
    df = spark.table("acme_prod.finance_bronze.silver_transformation")
    total = df.count()
    passing = df.filter("silver_transformation_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0