# Tests for customer_churn_model
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.analytics_bronze.customer_churn_model")

def test_no_null_keys(spark):
    df = spark.table("production.analytics_bronze.customer_churn_model")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.analytics_bronze.customer_churn_model").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=240)

def test_dq_valid_customer_churn_model_id(spark):
    df = spark.table("production.analytics_bronze.customer_churn_model")
    total = df.count()
    passing = df.filter("customer_churn_model_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0