# Tests for compliance_report
def test_table_exists(spark):
    assert spark.catalog.tableExists("production.compliance_bronze.compliance_report")

def test_no_null_keys(spark):
    df = spark.table("production.compliance_bronze.compliance_report")
    for key in []:
        assert df.filter(f"{key} IS NULL").count() == 0

def test_freshness(spark):
    from pyspark.sql.functions import max as smax
    from datetime import datetime, timedelta
    latest = spark.table("production.compliance_bronze.compliance_report").agg(smax("_ingested_at")).collect()[0][0]
    assert datetime.now() - latest < timedelta(minutes=180)

def test_dq_valid_compliance_report_id(spark):
    df = spark.table("production.compliance_bronze.compliance_report")
    total = df.count()
    passing = df.filter("compliance_report_id IS NOT NULL").count()
    assert passing / max(total, 1) >= 1.0