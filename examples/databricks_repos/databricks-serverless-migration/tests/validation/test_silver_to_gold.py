import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, current_timestamp, expr
from datetime import datetime, timedelta


TABLE_NAME = "acme_prod.finance_gold.silver_to_gold"

PRIMARY_KEY_COLUMNS = ["id"]

EXPECTED_COLUMNS = [
    "id",
    "account_id",
    "transaction_date",
    "amount",
    "currency",
    "status",
    "_processed_at",
]


@pytest.fixture(scope="module")
def table_df(spark):
    """Load the target table as a DataFrame for reuse across tests."""
    return spark.table(TABLE_NAME)


class TestSilverToGoldMigration:
    """Validation tests for the silver_to_gold pipeline migrated to Unity Catalog."""

    def test_table_exists(self, spark):
        """Verify the table was created in Unity Catalog."""
        tables = spark.sql(f"SHOW TABLES IN acme_prod.finance_gold LIKE 'silver_to_gold'")
        assert tables.count() > 0, (
            f"Table {TABLE_NAME} does not exist in Unity Catalog"
        )

    def test_schema_valid(self, table_df):
        """Verify expected columns exist in the table schema."""
        actual_columns = [field.name for field in table_df.schema.fields]
        missing_columns = [
            col_name for col_name in EXPECTED_COLUMNS if col_name not in actual_columns
        ]
        assert missing_columns == [], (
            f"Missing columns in {TABLE_NAME}: {missing_columns}. "
            f"Actual columns: {actual_columns}"
        )

    def test_no_null_keys(self, table_df):
        """Verify primary key columns are not null."""
        for key_col in PRIMARY_KEY_COLUMNS:
            null_count = table_df.filter(col(key_col).isNull()).count()
            assert null_count == 0, (
                f"Found {null_count} null values in primary key column '{key_col}' "
                f"in table {TABLE_NAME}"
            )

    def test_row_count(self, table_df):
        """Verify table has rows."""
        row_count = table_df.count()
        assert row_count > 0, (
            f"Table {TABLE_NAME} is empty (0 rows). Expected at least 1 row."
        )

    def test_freshness(self, table_df):
        """Verify _processed_at or _ingested_at is within last 24 hours."""
        freshness_columns = ["_processed_at", "_ingested_at"]
        actual_columns = [field.name for field in table_df.schema.fields]

        freshness_col = None
        for fc in freshness_columns:
            if fc in actual_columns:
                freshness_col = fc
                break

        assert freshness_col is not None, (
            f"No freshness column found in {TABLE_NAME}. "
            f"Expected one of {freshness_columns}, got columns: {actual_columns}"
        )

        max_timestamp_row = table_df.select(
            spark_max(col(freshness_col)).alias("max_ts")
        ).collect()[0]

        max_ts = max_timestamp_row["max_ts"]
        assert max_ts is not None, (
            f"All values in '{freshness_col}' are null in table {TABLE_NAME}"
        )

        cutoff = datetime.now() - timedelta(hours=24)
        assert max_ts >= cutoff, (
            f"Data in {TABLE_NAME} is stale. Most recent '{freshness_col}' is "
            f"{max_ts}, which is older than 24 hours (cutoff: {cutoff})"
        )

    def test_no_duplicates(self, table_df):
        """Verify no duplicate records on key columns."""
        total_count = table_df.count()
        distinct_count = table_df.select(
            *[col(key_col) for key_col in PRIMARY_KEY_COLUMNS]
        ).distinct().count()

        duplicate_count = total_count - distinct_count
        assert duplicate_count == 0, (
            f"Found {duplicate_count} duplicate records on key columns "
            f"{PRIMARY_KEY_COLUMNS} in table {TABLE_NAME}. "
            f"Total rows: {total_count}, Distinct keys: {distinct_count}"
        )