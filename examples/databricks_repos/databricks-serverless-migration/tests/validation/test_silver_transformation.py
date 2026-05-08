import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, current_timestamp


TABLE_NAME = "acme_prod.finance_silver.silver_transformation"

PRIMARY_KEY_COLUMNS = ["id"]

EXPECTED_COLUMNS = [
    "id",
    "account_id",
    "transaction_date",
    "amount",
    "currency",
    "category",
    "status",
    "_ingested_at",
    "_processed_at",
]


@pytest.fixture(scope="module")
def table_df(spark):
    """Load the target table as a DataFrame for reuse across tests."""
    return spark.table(TABLE_NAME)


class TestSilverTransformationMigration:
    """Validation tests for the migrated silver_transformation pipeline."""

    def test_table_exists(self, spark):
        """Verify the table was created in Unity Catalog."""
        tables = [
            t.name
            for t in spark.catalog.listTables(
                "acme_prod.finance_silver"
            )
        ]
        assert "silver_transformation" in tables, (
            f"Table 'silver_transformation' not found in catalog "
            f"'acme_prod.finance_silver'. Available tables: {tables}"
        )

    def test_schema_valid(self, table_df):
        """Verify expected columns exist in the table schema."""
        actual_columns = [field.name for field in table_df.schema.fields]
        missing_columns = [
            c for c in EXPECTED_COLUMNS if c not in actual_columns
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

        freshness_column = None
        if "_processed_at" in actual_columns:
            freshness_column = "_processed_at"
        elif "_ingested_at" in actual_columns:
            freshness_column = "_ingested_at"

        assert freshness_column is not None, (
            "Neither '_ingested_at' nor '_processed_at' column found in table. "
            f"Available columns: {actual_columns}"
        )

        max_timestamp_row = table_df.agg(
            spark_max(col(freshness_column)).alias("max_ts")
        ).collect()[0]

        max_timestamp = max_timestamp_row["max_ts"]
        assert max_timestamp is not None, (
            f"Column '{freshness_column}' contains only null values."
        )

        cutoff = datetime.now() - timedelta(hours=24)
        assert max_timestamp >= cutoff, (
            f"Data is stale. Most recent '{freshness_column}' value is "
            f"{max_timestamp}, which is older than 24 hours (cutoff: {cutoff})."
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