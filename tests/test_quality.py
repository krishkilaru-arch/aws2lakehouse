"""Tests for aws2lakehouse.quality module."""

import json

import pytest

from aws2lakehouse.quality import (
    AlertManager,
    DQFramework,
    DQRule,
    FreshnessSLAMonitor,
    LineageTracker,
    RuleAction,
    Severity,
    VolumeAnomalyDetector,
    _validate_sql_condition,
)


class TestSQLValidation:
    """Tests for the SQL injection guard."""

    def test_safe_condition_passes(self):
        _validate_sql_condition("amount > 0 AND status IS NOT NULL")

    def test_drop_rejected(self):
        with pytest.raises(ValueError, match="dangerous"):
            _validate_sql_condition("1=1; DROP TABLE users")

    def test_alter_rejected(self):
        with pytest.raises(ValueError, match="dangerous"):
            _validate_sql_condition("1=1; ALTER TABLE users ADD COLUMN x INT")

    def test_insert_rejected(self):
        with pytest.raises(ValueError, match="dangerous"):
            _validate_sql_condition("1=1; INSERT INTO audit VALUES (1)")

    def test_delete_rejected(self):
        with pytest.raises(ValueError, match="dangerous"):
            _validate_sql_condition("1=1; DELETE FROM users")

    def test_grant_rejected(self):
        with pytest.raises(ValueError, match="dangerous"):
            _validate_sql_condition("1=1; GRANT ALL ON users TO attacker")

    def test_comment_dash_rejected(self):
        with pytest.raises(ValueError, match="dangerous"):
            _validate_sql_condition("amount > 0 -- ignore rest")

    def test_semicolon_rejected(self):
        with pytest.raises(ValueError, match="dangerous"):
            _validate_sql_condition("amount > 0; SELECT 1")


class TestDQRule:
    def test_defaults(self):
        rule = DQRule(name="test", description="desc", condition="x > 0")
        assert rule.action == RuleAction.WARN
        assert rule.severity == Severity.WARNING
        assert rule.threshold == 1.0
        assert rule.tags == []

    def test_custom_fields(self):
        rule = DQRule(
            name="test",
            description="desc",
            condition="x > 0",
            action=RuleAction.FAIL,
            severity=Severity.CRITICAL,
            threshold=0.99,
            tags=["pk"],
        )
        assert rule.action == RuleAction.FAIL
        assert rule.threshold == 0.99


class TestDQFramework:
    def test_add_rule(self):
        dq = DQFramework()
        dq.add_rule(DQRule(name="test", description="d", condition="x > 0"), "orders")
        assert len(dq.rules["orders"]) == 1

    def test_add_standard_rules(self):
        dq = DQFramework()
        dq.add_standard_rules("orders", "order_id", ["amount", "status"])
        rules = dq.rules["orders"]
        names = [r.name for r in rules]
        assert "order_id_not_null" in names
        assert "order_id_unique" in names
        assert "amount_not_null" in names
        assert "status_not_null" in names

    def test_uniqueness_rule_uses_table_name(self):
        """Verify __self__ placeholder is replaced with actual table name."""
        dq = DQFramework()
        dq.add_standard_rules("orders", "order_id")
        unique_rule = [r for r in dq.rules["orders"] if "unique" in r.name][0]
        assert "orders" in unique_rule.condition
        assert "__self__" not in unique_rule.condition

    def test_validate_table_no_spark(self):
        """Without spark, returns empty results but doesn't crash."""
        dq = DQFramework()
        dq.add_rule(DQRule(name="r", description="d", condition="x > 0"), "tbl")
        results = dq.validate_table("catalog.schema.tbl")
        assert results == []

    def test_validate_table_no_rules(self):
        dq = DQFramework()
        results = dq.validate_table("catalog.schema.unknown_table")
        assert results == []

    def test_generate_expectations_sql(self):
        dq = DQFramework(catalog="prod")
        dq.add_rule(
            DQRule(name="valid_amount", description="d", condition="amount > 0", action=RuleAction.FAIL),
            "orders",
        )
        dq.add_rule(
            DQRule(name="not_null_id", description="d", condition="id IS NOT NULL", action=RuleAction.DROP),
            "orders",
        )
        sql = dq.generate_expectations_sql("orders")
        assert "CREATE OR REFRESH STREAMING TABLE" in sql
        assert "CONSTRAINT valid_amount" in sql
        assert "FAIL UPDATE" in sql
        assert "CONSTRAINT not_null_id" in sql
        assert "DROP ROW" in sql

    def test_generate_expectations_sql_no_rules(self):
        dq = DQFramework()
        sql = dq.generate_expectations_sql("unknown")
        assert "No DQ rules" in sql

    def test_generate_monitoring_dashboard_sql(self):
        dq = DQFramework(catalog="analytics")
        sql = dq.generate_monitoring_dashboard_sql()
        assert "analytics.audit.dq_summary" in sql
        assert "CREATE OR REPLACE VIEW" in sql


class TestLineageTracker:
    def test_generate_lineage_table_sql(self):
        lt = LineageTracker(catalog="prod")
        sql = lt.generate_lineage_table_sql()
        assert "prod.audit.pipeline_lineage" in sql
        assert "pipeline_name STRING" in sql
        assert "USING DELTA" in sql


class TestAlertManager:
    def test_add_alert(self):
        am = AlertManager()
        am.add_alert("test_alert", "status == 'FAILED'", Severity.CRITICAL, ["email@co.com"])
        assert len(am.alert_rules) == 1
        assert am.alert_rules[0]["severity"] == "critical"

    def test_generate_alert_config(self):
        am = AlertManager()
        am.add_alert("a1", "cond", Severity.WARNING, ["ch1"])
        config = am.generate_alert_config()
        data = json.loads(config)
        assert "alerts" in data
        assert len(data["alerts"]) == 1

    def test_add_standard_pipeline_alerts(self):
        am = AlertManager()
        am.add_standard_pipeline_alerts("loan_etl", "owner@co.com")
        assert len(am.alert_rules) == 3
        names = [r["name"] for r in am.alert_rules]
        assert "loan_etl_failure" in names
        assert "loan_etl_sla_breach" in names
        assert "loan_etl_dq_failure" in names


class TestFreshnessSLAMonitor:
    def test_no_tables(self):
        monitor = FreshnessSLAMonitor()
        sql = monitor.generate_monitoring_view_sql()
        assert "No tables" in sql

    def test_add_table_and_generate_view(self):
        monitor = FreshnessSLAMonitor(catalog="prod")
        monitor.add_table("prod.bronze.orders", sla_minutes=60, owner="team")
        sql = monitor.generate_monitoring_view_sql()
        assert "prod.observability.v_freshness_monitor" in sql
        assert "prod.bronze.orders" in sql
        assert "60" in sql
        assert "BREACHED" in sql

    def test_generate_alert_query(self):
        monitor = FreshnessSLAMonitor(catalog="prod")
        sql = monitor.generate_alert_query()
        assert "sla_status = 'BREACHED'" in sql


class TestVolumeAnomalyDetector:
    def test_no_tables(self):
        detector = VolumeAnomalyDetector()
        sql = detector.generate_anomaly_view_sql()
        assert "No tables" in sql

    def test_add_table_and_generate_view(self):
        detector = VolumeAnomalyDetector(catalog="prod")
        detector.add_table("prod.bronze.orders", low_threshold=0.3, high_threshold=3.0)
        sql = detector.generate_anomaly_view_sql()
        assert "prod.observability.v_volume_anomalies" in sql
        assert "LOW_VOLUME" in sql
        assert "HIGH_VOLUME" in sql
        assert "ZERO_ROWS" in sql

    def test_generate_alert_query(self):
        detector = VolumeAnomalyDetector(catalog="prod")
        sql = detector.generate_alert_query()
        assert "volume_status != 'NORMAL'" in sql

    def test_generate_history_table_sql(self):
        detector = VolumeAnomalyDetector(catalog="prod")
        sql = detector.generate_history_table_sql()
        assert "volume_metrics_history" in sql
        assert "USING DELTA" in sql
