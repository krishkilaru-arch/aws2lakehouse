"""Tests for the agent module (offline — no API calls)."""

from unittest.mock import MagicMock, patch

from aws2lakehouse.agent import MigrationAgent, MigrationResult
from aws2lakehouse.agent.prompts import (
    CODE_TRANSFORM_PROMPT,
    QUALITY_INFERENCE_PROMPT,
    SQL_TRANSFORM_PROMPT,
    SYSTEM_PROMPT,
)
from aws2lakehouse.agent.tools import TOOL_DEFINITIONS, AgentTools


class TestAgentTools:
    """Test tool execution (deterministic, no LLM)."""

    def test_tool_definitions_valid(self):
        """All tool definitions have required fields."""
        for tool in TOOL_DEFINITIONS:
            assert "name" in tool
            assert "description" in tool
            assert "input_schema" in tool
            assert tool["input_schema"]["type"] == "object"
            assert "properties" in tool["input_schema"]

    def test_write_notebook(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path), catalog="test", org="test_org")
        result = tools.execute("write_notebook", {
            "path": "src/pipelines/trading/bronze/etl.py",
            "code": "# Databricks notebook source\nprint('hello')",
            "description": "Test notebook",
        })
        assert result.success
        assert "etl.py" in result.artifacts[0]
        assert (tmp_path / "src" / "pipelines" / "trading" / "bronze" / "etl.py").exists()

    def test_write_notebook_rejects_traversal(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path))
        result = tools.execute("write_notebook", {
            "path": "../../etc/passwd",
            "code": "malicious",
            "description": "hack",
        })
        assert not result.success
        assert "traversal" in result.output.lower()

    def test_write_job_yaml(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path))
        result = tools.execute("write_job_yaml", {
            "name": "ingest_trades",
            "yaml_content": "resources:\n  jobs:\n    ingest_trades:\n      name: Ingest Trades",
        })
        assert result.success
        assert (tmp_path / "resources" / "jobs" / "ingest_trades.yml").exists()

    def test_write_quality_rules(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path))
        result = tools.execute("write_quality_rules", {
            "table_name": "production.trading.bronze_trades",
            "rules": [
                {"name": "not_null_trade_id", "condition": "trade_id IS NOT NULL", "action": "fail"},
                {"name": "positive_amount", "condition": "amount > 0", "action": "warn"},
            ],
        })
        assert result.success
        assert (tmp_path / "quality" / "bronze_trades_expectations.sql").exists()

    def test_write_governance_sql(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path))
        result = tools.execute("write_governance_sql", {
            "table_name": "production.risk.positions",
            "sql": "ALTER TABLE production.risk.positions SET TAGS ('classification' = 'mnpi');",
        })
        assert result.success
        assert (tmp_path / "governance" / "positions_governance.sql").exists()

    def test_write_validation_test(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path))
        result = tools.execute("write_validation_test", {
            "pipeline_name": "ingest_trades",
            "target_table": "production.trading.bronze_trades",
            "validation_code": "# Validation notebook\nassert spark.table('t').count() > 0",
        })
        assert result.success
        assert (tmp_path / "tests" / "validation" / "test_ingest_trades.py").exists()

    def test_analyze_complexity(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path))
        result = tools.execute("analyze_complexity", {
            "source_code": """
import boto3
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
df = spark.read.format("kafka").option("subscribe", "trades").load()
df.writeStream.format("delta").start()
""",
        })
        assert result.success
        import json
        data = json.loads(result.output)
        assert "category" in data
        assert "has_streaming" in data
        assert data["has_streaming"] is True

    def test_report_progress(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path))
        result = tools.execute("report_progress", {
            "pipeline_name": "ingest_trades",
            "status": "completed",
            "artifacts": ["src/pipelines/trading/bronze/etl.py"],
            "notes": "Converted Glue DynamicFrame to Spark DataFrame",
        })
        assert result.success
        assert len(tools.progress) == 1
        assert tools.progress[0]["status"] == "completed"

    def test_unknown_tool(self, tmp_path):
        tools = AgentTools(output_dir=str(tmp_path))
        result = tools.execute("nonexistent_tool", {})
        assert not result.success
        assert "Unknown" in result.output


class TestPrompts:
    """Test that prompts are well-formed."""

    def test_system_prompt_has_rules(self):
        assert "Never fabricate" in SYSTEM_PROMPT
        assert "medallion" in SYSTEM_PROMPT
        assert "Unity Catalog" in SYSTEM_PROMPT

    def test_code_transform_prompt_has_placeholders(self):
        formatted = CODE_TRANSFORM_PROMPT.format(
            source_type="glue",
            source_code="print('hello')",
            catalog="production",
            schema="trading_bronze",
            domain="trading",
            layer="bronze",
        )
        assert "glue" in formatted
        assert "production" in formatted

    def test_sql_transform_prompt_has_redshift_rules(self):
        assert "GETDATE()" in SQL_TRANSFORM_PROMPT
        assert "DATEADD()" in SQL_TRANSFORM_PROMPT
        assert "DISTKEY" in SQL_TRANSFORM_PROMPT

    def test_quality_inference_prompt_has_rules(self):
        assert "NOT NULL" in QUALITY_INFERENCE_PROMPT
        assert "UNIQUE" in QUALITY_INFERENCE_PROMPT
        assert "Freshness" in QUALITY_INFERENCE_PROMPT


class TestMigrationAgent:
    """Test agent initialization and offline behavior."""

    def test_init_with_api_key(self, tmp_path):
        agent = MigrationAgent(
            catalog="test",
            org="test_org",
            output_dir=str(tmp_path),
            api_key="sk-test-fake-key",
        )
        assert agent.catalog == "test"
        assert agent.org == "test_org"

    def test_init_without_credentials_raises(self, tmp_path):
        import os
        # Ensure no env var
        old = os.environ.pop("ANTHROPIC_API_KEY", None)
        try:
            try:
                MigrationAgent(
                    catalog="test",
                    output_dir=str(tmp_path),
                )
                raise AssertionError("Should have raised")
            except ValueError as e:
                assert "api_key" in str(e).lower() or "ANTHROPIC_API_KEY" in str(e)
        finally:
            if old:
                os.environ["ANTHROPIC_API_KEY"] = old

    def test_infer_name_from_dag_id(self, tmp_path):
        agent = MigrationAgent(
            catalog="test", output_dir=str(tmp_path), api_key="fake"
        )
        name = agent._infer_name("dag_id = 'daily_trade_ingestion'", "airflow")
        assert name == "daily_trade_ingestion"

    def test_infer_name_fallback(self, tmp_path):
        agent = MigrationAgent(
            catalog="test", output_dir=str(tmp_path), api_key="fake"
        )
        name = agent._infer_name("x = 1", "glue")
        assert name == "migrated_glue_pipeline"

    @patch("aws2lakehouse.agent.orchestrator._AnthropicClient")
    def test_migrate_pipeline_calls_agent(self, mock_client_cls, tmp_path):
        """Test the agent loop with a mocked Claude response."""
        # Mock Claude responding with tool calls then done
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        # First response: tool call to write_notebook
        mock_client.create_message.side_effect = [
            # Turn 1: analyze + write notebook
            {
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool_1",
                        "name": "write_notebook",
                        "input": {
                            "path": "src/pipelines/default/bronze/test.py",
                            "code": "# Databricks notebook source\nprint('migrated')",
                            "description": "Migrated pipeline",
                        },
                    },
                    {
                        "type": "tool_use",
                        "id": "tool_2",
                        "name": "report_progress",
                        "input": {
                            "pipeline_name": "test_pipeline",
                            "status": "completed",
                        },
                    },
                ],
                "usage": {"input_tokens": 1000, "output_tokens": 500},
                "stop_reason": "tool_use",
            },
            # Turn 2: done (text only)
            {
                "content": [{"type": "text", "text": "Migration complete."}],
                "usage": {"input_tokens": 200, "output_tokens": 50},
                "stop_reason": "end_turn",
            },
        ]

        agent = MigrationAgent(
            catalog="test", output_dir=str(tmp_path), api_key="fake"
        )
        agent._client = mock_client

        result = agent.migrate_pipeline(
            source_code="from awsglue.context import GlueContext\nglueContext.write_dynamic_frame(df)",
            source_type="glue",
            domain="default",
        )

        assert result.status == "completed"
        assert len(result.artifacts) >= 1
        assert result.notebook_code == "# Databricks notebook source\nprint('migrated')"
        assert result.token_usage["total_tokens"] == 1750


class TestMigrationResult:
    """Test result dataclasses."""

    def test_migration_result_defaults(self):
        r = MigrationResult(pipeline_name="test", status="completed")
        assert r.artifacts == []
        assert r.warnings == []
        assert r.notebook_code == ""

    def test_batch_result_summary(self):
        from aws2lakehouse.agent.orchestrator import BatchMigrationResult
        batch = BatchMigrationResult(total=10, completed=8, failed=1, needs_review=1)
        assert "8/10" in batch.summary
        assert "1 failed" in batch.summary
