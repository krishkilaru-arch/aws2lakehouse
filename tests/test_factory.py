"""Tests for the Pipeline Factory engine."""

import pytest

from aws2lakehouse.factory import (
    Classification,
    GovernanceConfig,
    PipelineArtifacts,
    PipelineFactory,
    PipelineSpec,
    QualityAction,
    QualityRule,
    ScheduleConfig,
    SourceConfig,
    SourceType,
    TargetConfig,
    TargetMode,
)


@pytest.fixture
def simple_spec():
    """A minimal batch pipeline spec."""
    spec = PipelineSpec(name="test_pipeline", domain="analytics", owner="test@co.com")
    spec.source = SourceConfig(type=SourceType.AUTO_LOADER, config={"format": "json"})
    spec.target = TargetConfig(
        catalog="test_catalog", schema="bronze", table="test_table", mode=TargetMode.BATCH
    )
    spec.schedule = ScheduleConfig(cron="0 6 * * *", sla_minutes=60)
    return spec


@pytest.fixture
def streaming_spec():
    """A Kafka streaming pipeline spec."""
    spec = PipelineSpec(name="trade_stream", domain="risk", owner="risk@co.com")
    spec.source = SourceConfig(
        type=SourceType.KAFKA,
        config={"topic": "trades", "secret_scope": "kafka-prod"},
    )
    spec.target = TargetConfig(
        catalog="production", schema="risk_bronze", table="trade_events",
        mode=TargetMode.STREAMING,
    )
    spec.governance = GovernanceConfig(
        classification=Classification.MNPI,
        mnpi_columns=["trade_price", "volume"],
    )
    spec.schedule = ScheduleConfig(sla_minutes=5)
    return spec


@pytest.fixture
def merge_spec():
    """A merge/upsert pipeline spec."""
    spec = PipelineSpec(name="customer_dim", domain="customer", owner="cust@co.com")
    spec.source = SourceConfig(type=SourceType.JDBC, config={"source_table": "customers"})
    spec.target = TargetConfig(
        catalog="production", schema="silver", table="dim_customer",
        mode=TargetMode.MERGE, merge_keys=["customer_id"],
    )
    spec.quality = [
        QualityRule(name="not_null_id", condition="customer_id IS NOT NULL", action=QualityAction.FAIL),
        QualityRule(name="valid_email", condition="email LIKE '%@%'", action=QualityAction.WARN),
    ]
    return spec


@pytest.fixture
def factory():
    return PipelineFactory(catalog="test_catalog", environment="dev")


class TestPipelineSpec:
    def test_from_dict_minimal(self):
        data = {"name": "test", "source": {"type": "auto_loader"}}
        spec = PipelineSpec.from_dict(data)
        assert spec.name == "test"
        assert spec.source.type == SourceType.AUTO_LOADER
        assert spec.domain == "default"

    def test_from_dict_full(self):
        data = {
            "name": "full_pipeline",
            "domain": "risk",
            "owner": "team@co.com",
            "layer": "silver",
            "source": {"type": "kafka", "config": {"topic": "events"}},
            "target": {"catalog": "prod", "schema": "risk", "table": "events",
                       "mode": "streaming"},
            "quality": [{"name": "not_null", "condition": "id IS NOT NULL", "action": "fail"}],
            "governance": {"classification": "mnpi", "mnpi_columns": ["price"]},
            "schedule": {"cron": "0 * * * *"},
            "tags": {"env": "prod"},
        }
        spec = PipelineSpec.from_dict(data)
        assert spec.domain == "risk"
        assert spec.source.type == SourceType.KAFKA
        assert spec.target.mode == TargetMode.STREAMING
        assert len(spec.quality) == 1
        assert spec.governance.classification == Classification.MNPI

    def test_from_dict_missing_name(self):
        """from_dict should raise ValueError when 'name' is missing."""
        with pytest.raises(ValueError, match="requires a 'name'"):
            PipelineSpec.from_dict({"domain": "risk"})

    def test_from_dict_empty_name(self):
        """from_dict should raise ValueError when 'name' is empty string."""
        with pytest.raises(ValueError, match="requires a 'name'"):
            PipelineSpec.from_dict({"name": ""})

    def test_to_yaml_roundtrip(self):
        spec = PipelineSpec(name="roundtrip_test", domain="test")
        yaml_str = spec.to_yaml()
        assert "name: roundtrip_test" in yaml_str
        assert "domain: test" in yaml_str

    def test_source_types_all_valid(self):
        for st in SourceType:
            spec = PipelineSpec.from_dict({"name": "t", "source": {"type": st.value}})
            assert spec.source.type == st


class TestPipelineFactory:
    def test_generate_returns_all_artifacts(self, factory, simple_spec):
        artifacts = factory.generate(simple_spec)
        assert isinstance(artifacts, PipelineArtifacts)
        assert len(artifacts.notebook_code) > 0
        assert len(artifacts.job_yaml) > 0
        assert len(artifacts.governance_sql) > 0
        assert len(artifacts.test_code) > 0
        assert len(artifacts.monitoring_sql) > 0

    def test_notebook_has_header(self, factory, simple_spec):
        artifacts = factory.generate(simple_spec)
        assert "Databricks notebook source" in artifacts.notebook_code
        assert simple_spec.name in artifacts.notebook_code

    def test_notebook_has_source_code(self, factory, simple_spec):
        artifacts = factory.generate(simple_spec)
        assert "Auto Loader" in artifacts.notebook_code or "cloudFiles" in artifacts.notebook_code

    def test_streaming_notebook(self, factory, streaming_spec):
        artifacts = factory.generate(streaming_spec)
        assert "readStream" in artifacts.notebook_code
        assert "writeStream" in artifacts.notebook_code
        assert "kafka" in artifacts.notebook_code.lower()

    def test_merge_notebook(self, factory, merge_spec):
        artifacts = factory.generate(merge_spec)
        assert "merge" in artifacts.notebook_code.lower() or "DeltaTable" in artifacts.notebook_code

    def test_job_yaml_has_schedule(self, factory, simple_spec):
        artifacts = factory.generate(simple_spec)
        assert "quartz_cron_expression" in artifacts.job_yaml

    def test_job_yaml_has_task(self, factory, simple_spec):
        artifacts = factory.generate(simple_spec)
        assert "task_key" in artifacts.job_yaml
        assert "notebook_task" in artifacts.job_yaml

    def test_governance_sql_has_tags(self, factory, streaming_spec):
        artifacts = factory.generate(streaming_spec)
        assert "SET TAGS" in artifacts.governance_sql
        assert "mnpi" in artifacts.governance_sql

    def test_governance_mnpi_columns_tagged(self, factory, streaming_spec):
        artifacts = factory.generate(streaming_spec)
        assert "trade_price" in artifacts.governance_sql
        assert "volume" in artifacts.governance_sql

    def test_dq_sql_with_rules(self, factory, merge_spec):
        artifacts = factory.generate(merge_spec)
        assert "not_null_id" in artifacts.dq_sql
        assert "EXPECT" in artifacts.dq_sql

    def test_dq_sql_without_rules(self, factory, simple_spec):
        artifacts = factory.generate(simple_spec)
        assert "No quality rules" in artifacts.dq_sql or len(artifacts.dq_sql) > 0

    def test_test_code_has_assertions(self, factory, merge_spec):
        artifacts = factory.generate(merge_spec)
        assert "def test_table_exists" in artifacts.test_code
        assert "def test_no_null_keys" in artifacts.test_code  # merge_keys defined
        assert "def test_freshness" in artifacts.test_code

    def test_test_code_no_null_key_test_without_merge_keys(self, factory, simple_spec):
        """F5: test_no_null_keys should only appear when merge_keys are defined."""
        artifacts = factory.generate(simple_spec)
        assert "test_no_null_keys" not in artifacts.test_code

    def test_monitoring_sql_has_freshness(self, factory, simple_spec):
        artifacts = factory.generate(simple_spec)
        assert "Freshness" in artifacts.monitoring_sql or "staleness" in artifacts.monitoring_sql

    def test_monitoring_sql_has_volume(self, factory, simple_spec):
        artifacts = factory.generate(simple_spec)
        assert "volume" in artifacts.monitoring_sql.lower() or "row_count" in artifacts.monitoring_sql


class TestQuartzConversion:
    """Test the _to_quartz static method."""

    def test_basic_daily(self):
        result = PipelineFactory._to_quartz("0 6 * * *")
        assert result == "0 0 6 * * ? *"

    def test_dow_specified(self):
        # cron DOW 1 (Monday) -> Quartz DOW 2 (Monday)
        result = PipelineFactory._to_quartz("0 6 * * 1")
        assert result == "0 0 6 ? * 2 *"

    def test_dom_specified(self):
        result = PipelineFactory._to_quartz("0 6 15 * *")
        assert result == "0 0 6 15 * ? *"

    def test_both_dom_dow(self):
        """F4: When both DOM and DOW are specified, DOW takes precedence."""
        # cron DOW 1 (Monday) -> Quartz DOW 2 (Monday)
        result = PipelineFactory._to_quartz("0 6 15 * 1")
        assert result == "0 0 6 ? * 2 *"

    def test_passthrough_quartz(self):
        """Already 7-field Quartz should pass through."""
        result = PipelineFactory._to_quartz("0 0 6 ? * 1 *")
        assert result == "0 0 6 ? * 1 *"

    def test_named_dow_unchanged(self):
        """Named days like MON should not be offset."""
        result = PipelineFactory._to_quartz("0 6 * * MON")
        assert result == "0 0 6 ? * MON *"

    def test_dow_sun_zero(self):
        """Cron DOW 0 (Sunday) -> Quartz DOW 1 (Sunday)."""
        result = PipelineFactory._to_quartz("30 2 * * 0")
        assert result == "0 30 2 ? * 1 *"


class TestMaskTypeInference:
    """Test the _infer_mask_type static method."""

    def test_string_literal(self):
        assert PipelineFactory._infer_mask_type("'[REDACTED]'") == "STRING"

    def test_double(self):
        assert PipelineFactory._infer_mask_type("0.00") == "DOUBLE"

    def test_bigint(self):
        assert PipelineFactory._infer_mask_type("0") == "BIGINT"

    def test_decimal(self):
        assert PipelineFactory._infer_mask_type("CAST(0 AS DECIMAL(10,2))") == "DECIMAL(18,2)"

    def test_boolean(self):
        assert PipelineFactory._infer_mask_type("FALSE") == "BOOLEAN"

    def test_null_defaults_string(self):
        assert PipelineFactory._infer_mask_type("NULL") == "STRING"
