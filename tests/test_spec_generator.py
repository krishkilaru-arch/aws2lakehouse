"""Tests for the SpecGenerator module."""

import pytest

from aws2lakehouse.factory import Classification, PipelineSpec, SourceType
from aws2lakehouse.factory.spec_generator import SpecGenerator


@pytest.fixture
def generator():
    return SpecGenerator(catalog="test_catalog")


class TestSpecGenerator:
    def test_from_inventory_basic(self, generator, sample_inventory):
        specs = generator.from_inventory(sample_inventory)
        assert len(specs) == len(sample_inventory)
        assert all(isinstance(s, PipelineSpec) for s in specs)

    def test_names_preserved(self, generator, sample_inventory):
        specs = generator.from_inventory(sample_inventory)
        names = {s.name for s in specs}
        expected = {p["name"] for p in sample_inventory}
        assert names == expected

    def test_catalog_applied(self, generator, sample_inventory):
        specs = generator.from_inventory(sample_inventory)
        for spec in specs:
            assert spec.target.catalog == "test_catalog"

    def test_source_type_mapping(self, generator, sample_inventory):
        specs = generator.from_inventory(sample_inventory)
        spec_map = {s.name: s for s in specs}

        # auto_loader inventory entry
        cust = spec_map["daily_customer_etl"]
        assert cust.source.type == SourceType.AUTO_LOADER

        # kafka
        trade = spec_map["trade_events_streaming"]
        assert trade.source.type == SourceType.KAFKA

        # jdbc
        pay = spec_map["payment_processor"]
        assert pay.source.type == SourceType.JDBC

    def test_classification_mapping(self, generator, sample_inventory):
        specs = generator.from_inventory(sample_inventory)
        spec_map = {s.name: s for s in specs}
        assert spec_map["trade_events_streaming"].governance.classification == Classification.MNPI
        assert spec_map["daily_customer_etl"].governance.classification == Classification.CONFIDENTIAL

    def test_domain_assigned(self, generator, sample_inventory):
        specs = generator.from_inventory(sample_inventory)
        for spec in specs:
            assert spec.domain != ""

    def test_empty_inventory(self, generator):
        specs = generator.from_inventory([])
        assert specs == []

    def test_single_pipeline(self, generator):
        inv = [{"name": "solo", "domain": "test", "source_type": "auto_loader",
                "source_config": {}, "schedule": "0 * * * *", "sla_minutes": 30,
                "business_impact": "low", "owner": "me", "classification": "internal",
                "mnpi_columns": [], "pii_columns": []}]
        specs = generator.from_inventory(inv)
        assert len(specs) == 1
        assert specs[0].name == "solo"

    def test_from_table_sql(self, generator):
        sql = generator.from_table_sql("prod.tracking.inventory")
        assert "SELECT" in sql
        assert "prod.tracking.inventory" in sql
        assert "migration_status" in sql


class TestSpecGeneratorYAMLOutput:
    def test_write_specs_to_directory(self, generator, sample_inventory, tmp_dir):
        import os
        specs = generator.from_inventory(sample_inventory)
        generator.write_specs_to_directory(specs, tmp_dir)
        # Should have written YAML files
        [f for f in os.listdir(tmp_dir) if f.endswith((".yml", ".yaml"))]
        # May organize by domain subdirs
        yaml_count = 0
        for _root, _dirs, files in os.walk(tmp_dir):
            yaml_count += len([f for f in files if f.endswith((".yml", ".yaml"))])
        assert yaml_count >= len(specs)
