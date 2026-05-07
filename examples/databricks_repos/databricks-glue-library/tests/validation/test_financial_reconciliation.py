# Tests for financial_reconciliation
def test_table_exists(spark):
    assert spark.catalog.tableExists("acme_prod.finance_bronze.financial_reconciliation")

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("acme_prod.finance_bronze.financial_reconciliation").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=60)

def test_dq_valid_financial_reconciliation_id(spark):
    df = spark.table("acme_prod.finance_bronze.financial_reconciliation")
    total = df.count()
    passing = df.filter("financial_reconciliation_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0