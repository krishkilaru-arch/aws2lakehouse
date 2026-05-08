"""Shared fixtures for migration validation tests."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Get or create SparkSession."""
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def catalog():
    return "acme_prod"
