# Tests for feature_store_builder
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.finance_bronze.feature_store_builder")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.finance_bronze.feature_store_builder").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_feature_store_builder_id(spark):
    df = spark.table("acme_prod.finance_bronze.feature_store_builder")
    total = df.count()
    passing = df.filter("feature_store_builder_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0