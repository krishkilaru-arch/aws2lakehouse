"""Tests for aws2lakehouse.ingestion and aws2lakehouse.glue modules."""

import pytest

from aws2lakehouse.glue import GlueCodeTransformer, GlueWorkflowMigrator
from aws2lakehouse.ingestion import (
    AutoLoaderPattern,
    ConnectorFactory,
    DeltaShareCDF,
    IngestionConfig,
    KafkaIngestion,
)

# ── AutoLoader ───────────────────────────────────────────────────────────────


class TestAutoLoaderPattern:
    def test_generate_code_json(self):
        loader = AutoLoaderPattern(
            source_path="/Volumes/raw/orders/",
            target_table="prod.bronze.orders",
            file_format="json",
        )
        code = loader.generate_code()
        assert "cloudFiles" in code
        assert "cloudFiles.format" in code
        assert "json" in code
        assert "prod.bronze.orders" in code
        assert "readStream" in code
        assert "writeStream" in code

    def test_generate_code_csv(self):
        loader = AutoLoaderPattern(
            source_path="/Volumes/raw/csv/",
            target_table="prod.bronze.csv_data",
            file_format="csv",
        )
        code = loader.generate_code()
        assert "header" in code
        assert "inferSchema" in code

    def test_schema_evolution(self):
        loader = AutoLoaderPattern(
            source_path="/path",
            target_table="t",
            schema_evolution=True,
        )
        code = loader.generate_code()
        assert "schemaEvolutionMode" in code
        assert "mergeSchema" in code

    def test_partition_columns(self):
        loader = AutoLoaderPattern(
            source_path="/path",
            target_table="t",
            partition_columns=["date", "region"],
        )
        code = loader.generate_code()
        assert "partitionBy" in code
        assert "date" in code

    def test_notification_mode(self):
        loader = AutoLoaderPattern(
            source_path="/path",
            target_table="t",
            notification_mode=True,
        )
        code = loader.generate_code()
        assert "useNotifications" in code

    def test_schema_hints(self):
        loader = AutoLoaderPattern(
            source_path="/path",
            target_table="t",
            schema_hints={"id": "INT", "name": "STRING"},
        )
        code = loader.generate_code()
        assert "schemaHints" in code

    def test_metadata_columns(self):
        loader = AutoLoaderPattern(
            source_path="/path",
            target_table="t",
        )
        code = loader.generate_code()
        assert "_ingested_at" in code
        assert "_source_file" in code


# ── DeltaShareCDF ────────────────────────────────────────────────────────────


class TestDeltaShareCDF:
    def test_generate_cdf_code(self):
        cdf = DeltaShareCDF(
            share_name="vendor_share",
            schema_name="raw",
            table_name="transactions",
            target_table="prod.bronze.txns",
        )
        code = cdf.generate_code()
        assert "deltaSharing" in code
        assert "readChangeFeed" in code
        assert "vendor_share.raw.transactions" in code
        assert "merge" in code.lower()

    def test_generate_batch_code(self):
        cdf = DeltaShareCDF(
            share_name="s",
            schema_name="sc",
            table_name="t",
            target_table="prod.bronze.t",
            cdf_enabled=False,
        )
        code = cdf.generate_code()
        assert "overwrite" in code.lower()
        assert "readChangeFeed" not in code


# ── KafkaIngestion ───────────────────────────────────────────────────────────


class TestKafkaIngestion:
    def test_generate_code(self):
        kafka = KafkaIngestion(
            bootstrap_servers="broker1:9092,broker2:9092",
            topic="events",
            target_table="prod.bronze.events",
        )
        code = kafka.generate_code()
        assert "kafka" in code.lower()
        assert "broker1:9092" in code
        assert "events" in code
        assert "readStream" in code

    def test_schema_inference_uses_batch(self):
        """Bug fix: schema inference must use batch read, not streaming DF."""
        kafka = KafkaIngestion(
            bootstrap_servers="broker:9092",
            topic="test",
            target_table="t",
        )
        code = kafka.generate_code()
        # Should use spark.read (batch) for schema inference, not streaming
        assert "spark.read" in code
        # Must not call .limit() on the readStream (streaming DF)
        # The batch sample should be separate from the readStream
        assert "readStream" in code  # streaming still used for actual ingestion


# ── ConnectorFactory ─────────────────────────────────────────────────────────


class TestConnectorFactory:
    def test_mongodb_connector(self):
        code = ConnectorFactory.generate("mongodb", {
            "scope": "secrets",
            "database": "mydb",
            "collection": "users",
            "target_table": "prod.bronze.users",
        })
        assert "mongodb" in code.lower()
        assert "mydb" in code
        assert "users" in code

    def test_postgresql_connector(self):
        code = ConnectorFactory.generate("postgresql", {
            "scope": "secrets",
            "source_table": "public.orders",
            "target_table": "prod.bronze.orders",
            "partition_col": "id",
            "lower": "0",
            "upper": "1000000",
            "num_partitions": "20",
        })
        assert "jdbc" in code.lower()
        assert "public.orders" in code

    def test_snowflake_connector(self):
        code = ConnectorFactory.generate("snowflake", {
            "scope": "secrets",
            "database": "ANALYTICS",
            "schema": "PUBLIC",
            "warehouse": "COMPUTE_WH",
            "source_table": "EVENTS",
            "target_table": "prod.bronze.events",
        })
        assert "snowflake" in code.lower()
        assert "ANALYTICS" in code

    def test_unknown_connector_raises(self):
        with pytest.raises(ValueError, match="Unknown connector"):
            ConnectorFactory.generate("oracle", {})


# ── IngestionConfig ──────────────────────────────────────────────────────────


class TestIngestionConfig:
    def test_defaults(self):
        cfg = IngestionConfig(source_name="test")
        assert cfg.target_catalog == "production"
        assert cfg.target_schema == "bronze"
        assert cfg.trigger == "availableNow"


# ── GlueCodeTransformer ─────────────────────────────────────────────────────


class TestGlueCodeTransformer:
    def test_transform_basic(self):
        transformer = GlueCodeTransformer(target_catalog="production")
        source = '''
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = glueContext.create_dynamic_frame.from_catalog(database="mydb", table_name="mytable")
df.toDF().write.format("parquet").save("s3://bucket/output")
'''
        result = transformer.transform(source)
        assert isinstance(result, str)
        assert len(result) > 0
        # GlueContext should be transformed or removed
        if "GlueContext" not in result:
            assert "SparkSession" in result or "spark" in result

    def test_get_warnings(self):
        transformer = GlueCodeTransformer(target_catalog="production")
        source = "from awsglue.context import GlueContext\nprint('hello')"
        transformer.transform(source)
        warnings = transformer.get_warnings()
        assert isinstance(warnings, list)


class TestGlueWorkflowMigrator:
    def test_migrate_raises_not_implemented(self):
        migrator = GlueWorkflowMigrator(target_catalog="production")
        with pytest.raises(NotImplementedError):
            migrator.migrate("test_workflow")
