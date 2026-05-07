"""Unit tests for SCD2 merge logic."""
import pytest
from pyspark.sql import SparkSession
from acme_etl.transforms.scd2 import detect_changes, scd2_merge


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_detect_new_records(spark):
    source = spark.createDataFrame([
        (1, "Alice", "NYC"), (2, "Bob", "LA"), (3, "Charlie", "SF")
    ], ["id", "name", "city"])
    
    target = spark.createDataFrame([
        (1, "Alice", "NYC", True), (2, "Bob", "LA", True)
    ], ["id", "name", "city", "is_current"])
    
    changes = detect_changes(source, target, ["id"], ["name", "city"])
    assert changes["new"].count() == 1
    assert changes["new"].collect()[0]["name"] == "Charlie"


def test_detect_changes(spark):
    source = spark.createDataFrame([
        (1, "Alice", "Boston")  # Alice moved from NYC to Boston
    ], ["id", "name", "city"])
    
    target = spark.createDataFrame([
        (1, "Alice", "NYC", True)
    ], ["id", "name", "city", "is_current"])
    
    changes = detect_changes(source, target, ["id"], ["name", "city"])
    assert changes["changed"].count() == 1
    assert changes["unchanged"].count() == 0
