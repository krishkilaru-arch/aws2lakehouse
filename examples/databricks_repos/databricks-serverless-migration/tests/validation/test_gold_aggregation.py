import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, current_timestamp, expr
from datetime import datetime, timedelta


TABLE_NAME = "acme_prod.finance_gold.gold_aggregation"

PRIMARY_KEY_COLUMNS = ["aggregation_id"]

EXPECTED_COLUMNS = [
    "aggregation_id",
    "account_id",
    "period",
    "total_amount",
    "transaction_count",
    "avg_amount",
    "_processed_at",
]


@pytest.fixture(scope="module")
def df(spark):
    """Load the target table as a DataFrame for reuse across tests."""
    return spark.table(TABLE_NAME)


class TestGoldAggregationMigration:
    """Validation tests for the migrated gold_aggregation pipeline."""

    def test_table_exists(self, spark):
        """Verify the table was created in Unity Catalog."""
        tables = spark.sql(f"SHOW TABLES IN acme_prod.finance_gold LIKE 'gold_aggregation'")
        assert tables.count() > 0, (
            f"Table {TABLE_NAME} does not exist in Unity Catalog"
        )

    def test_schema_valid(self, df):
        """Verify expected columns exist in the table schema."""
        actual_columns = [field.name for field in df.schema.fields]
        missing_columns = [c for c in EXPECTED_COLUMNS if c not in actual_columns]
        assert not missing_columns, (
            f"Missing expected columns in {TABLE_NAME}: {missing_columns}. "
            f"Actual columns: {actual_columns}"
        )

    def test_no_null_keys(self, df):
        """Verify primary key columns are not null."""
        for key_col in PRIMARY_KEY_COLUMNS:
            null_count = df.filter(col(key_col).isNull()).count()
            assert null_count == 0, (
                f"Found {null_count} null values in primary key column '{key_col}' "
                f"in table {TABLE_NAME}"
            )

    def test_row_count(self, df):
        """Verify table has rows."""
        row_count = df.count()
        assert row_count > 0, (
            f"Table {TABLE_NAME} is empty (0 rows). "
            f"Expected at least 1 row after migration."
        )

    def test_freshness(self, df, spark):
        """Verify _processed_at or _ingested_at is within last 24 hours."""
        actual_columns = [field.name for field in df.schema.fields]

        if "_processed_at" in actual_columns:
            freshness_col = "_processed_at"
        elif "_ingested_at" in actual_columns:
            freshness_col = "_ingested_at"
        else:
            pytest.fail(
                f"Neither '_processed_at' nor '_ingested_at' column found in {TABLE_NAME}. "
                f"Available columns: {actual_columns}"
            )

        max_timestamp_row = df.agg(spark_max(col(freshness_col)).alias("max_ts")).collect()[0]
        max_timestamp = max_timestamp_row["max_ts"]

        assert max_timestamp is not None, (
            f"Column '{freshness_col}' contains only null values in {TABLE_NAME}"
        )

        cutoff = datetime.now() - timedelta(hours=24)
        assert max_timestamp >= cutoff, (
            f"Data freshness check failed for {TABLE_NAME}. "
            f"Most recent '{freshness_col}' is {max_timestamp}, "
            f"which is older than 24 hours (cutoff: {cutoff})"
        )

    def test_no_duplicates(self, df):
        """Verify no duplicate records on primary key columns."""
        total_count = df.count()
        distinct_count = df.select(*PRIMARY_KEY_COLUMNS).distinct().count()

        duplicate_count = total_count - distinct_count
        assert duplicate_count == 0, (
            f"Found {duplicate_count} duplicate records on key columns "
            f"{PRIMARY_KEY_COLUMNS} in table {TABLE_NAME}. "
            f"Total rows: {total_count}, Distinct keys: {distinct_count}"
        )