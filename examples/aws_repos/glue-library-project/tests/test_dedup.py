"""Unit tests for deduplication logic."""
import pytest
from pyspark.sql import SparkSession
from acme_etl.transforms.dedup import dedup_by_key, dedup_by_window


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_dedup_keeps_latest(spark):
    data = spark.createDataFrame([
        (1, "2024-01-01", "old"), (1, "2024-01-15", "new"),
        (2, "2024-01-10", "only")
    ], ["id", "updated_at", "value"])
    
    result = dedup_by_key(data, ["id"], "updated_at")
    assert result.count() == 2
    assert result.filter("id = 1").collect()[0]["value"] == "new"


def test_dedup_keeps_earliest(spark):
    data = spark.createDataFrame([
        (1, "2024-01-01", "first"), (1, "2024-01-15", "second"),
    ], ["id", "updated_at", "value"])
    
    result = dedup_by_key(data, ["id"], "updated_at", ascending=True)
    assert result.filter("id = 1").collect()[0]["value"] == "first"
