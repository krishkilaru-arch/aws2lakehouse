import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, current_timestamp, expr
from datetime import datetime, timedelta


TABLE_NAME = "acme_prod.finance_bronze.bronze_ingestion"

PRIMARY_KEY_COLUMNS = ["id"]

EXPECTED_COLUMNS = [
    "id",
    "source_system",
    "payload",
    "_ingested_at",
    "_processed_at",
]


@pytest.fixture(scope="module")
def table_df(spark):
    """Load the target table as a DataFrame for reuse across tests."""
    return spark.table(TABLE_NAME)


class TestBronzeIngestionMigration:
    """Validation tests for the migrated bronze_ingestion pipeline."""

    def test_table_exists(self, spark):
        """Verify the table was created in Unity Catalog."""
        tables = [
            t.name
            for t in spark.catalog.listTables(
                "acme_prod.finance_bronze"
            )
        ]
        assert "bronze_ingestion" in tables, (
            f"Table 'bronze_ingestion' not found in catalog 'acme_prod.finance_bronze'. "
            f"Available tables: {tables}"
        )

    def test_schema_valid(self, table_df):
        """Verify expected columns exist in the table schema."""
        actual_columns = [field.name for field in table_df.schema.fields]
        missing_columns = [
            col_name
            for col_name in EXPECTED_COLUMNS
            if col_name not in actual_columns
        ]
        assert not missing_columns, (
            f"Missing expected columns: {missing_columns}. "
            f"Actual columns: {actual_columns}"
        )

    def test_no_null_keys(self, table_df):
        """Verify primary key columns are not null."""
        for key_col in PRIMARY_KEY_COLUMNS:
            null_count = table_df.filter(col(key_col).isNull()).count()
            assert null_count == 0, (
                f"Primary key column '{key_col}' has {null_count} null values."
            )

    def test_row_count(self, table_df):
        """Verify table has rows (is not empty)."""
        row_count = table_df.count()
        assert row_count > 0, (
            f"Table '{TABLE_NAME}' is empty. Expected at least 1 row."
        )

    def test_freshness(self, table_df):
        """Verify _ingested_at or _processed_at is within last 24 hours."""
        actual_columns = [field.name for field in table_df.schema.fields]

        freshness_col = None
        if "_ingested_at" in actual_columns:
            freshness_col = "_ingested_at"
        elif "_processed_at" in actual_columns:
            freshness_col = "_processed_at"

        assert freshness_col is not None, (
            "Neither '_ingested_at' nor '_processed_at' column found in table schema. "
            f"Available columns: {actual_columns}"
        )

        max_timestamp_row = table_df.select(
            spark_max(col(freshness_col)).alias("max_ts")
        ).collect()[0]

        max_ts = max_timestamp_row["max_ts"]
        assert max_ts is not None, (
            f"Column '{freshness_col}' contains only null values."
        )

        cutoff = datetime.now() - timedelta(hours=24)
        assert max_ts >= cutoff, (
            f"Data freshness check failed. Most recent '{freshness_col}' is {max_ts}, "
            f"which is older than 24 hours (cutoff: {cutoff})."
        )

    def test_no_duplicates(self, table_df):
        """Verify no duplicate records on primary key columns."""
        total_count = table_df.count()
        distinct_count = table_df.select(
            *[col(c) for c in PRIMARY_KEY_COLUMNS]
        ).distinct().count()

        duplicate_count = total_count - distinct_count
        assert duplicate_count == 0, (
            f"Found {duplicate_count} duplicate records based on key columns "
            f"{PRIMARY_KEY_COLUMNS}. Total rows: {total_count}, "
            f"Distinct keys: {distinct_count}."
        )