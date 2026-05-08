"""Unit tests for data quality validators."""
import pytest
from pyspark.sql import SparkSession
from acme_datalib.quality.validators import DataValidator


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_check_not_null_passes(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    validator = DataValidator(df, "test")
    results = validator.check_not_null(["id", "name"]).validate(fail_on_error=False)
    assert all(r.passed for r in results)


def test_check_not_null_fails(spark):
    df = spark.createDataFrame([(1, "a"), (2, None)], ["id", "name"])
    validator = DataValidator(df, "test")
    results = validator.check_not_null(["name"]).validate(fail_on_error=False)
    assert not results[0].passed
    assert results[0].failed_records == 1


def test_check_range(spark):
    df = spark.createDataFrame([(1, 100.0), (2, -5.0), (3, 50.0)], ["id", "price"])
    validator = DataValidator(df, "test")
    results = validator.check_range("price", min_val=0.0, max_val=200.0).validate(fail_on_error=False)
    assert not results[0].passed  # -5.0 is out of range
    assert results[0].failed_records == 1
