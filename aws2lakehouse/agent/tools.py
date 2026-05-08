"""
Agent Tools — Functions the Claude agent can call during migration.

Each tool is a callable that performs a deterministic operation.
The agent decides WHEN and HOW to use these tools based on context.
"""

import json
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ─── Tool Definitions (for Claude tool_use) ────────────────────────────────

TOOL_DEFINITIONS = [
    {
        "name": "write_notebook",
        "description": (
            "Write a Databricks notebook (.py) to the output directory. "
            "The code should use # Databricks notebook source format with "
            "# COMMAND ---------- cell separators."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Relative path within the output repo (e.g., 'src/pipelines/trading/bronze/ingest_trades.py')",
                },
                "code": {
                    "type": "string",
                    "description": "Full notebook source code",
                },
                "description": {
                    "type": "string",
                    "description": "One-line description of what this notebook does",
                },
            },
            "required": ["path", "code", "description"],
        },
    },
    {
        "name": "write_job_yaml",
        "description": (
            "Write a Databricks Workflow job definition (YAML) to resources/jobs/. "
            "Uses Databricks Asset Bundle format."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Job name (snake_case)",
                },
                "yaml_content": {
                    "type": "string",
                    "description": "Full YAML content for the job definition",
                },
            },
            "required": ["name", "yaml_content"],
        },
    },
    {
        "name": "write_quality_rules",
        "description": (
            "Write data quality expectations SQL for a table. "
            "Uses Databricks SDP (Streaming Data Pipelines) EXPECT syntax."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name (catalog.schema.table)",
                },
                "rules": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "condition": {"type": "string"},
                            "action": {"type": "string", "enum": ["warn", "drop", "quarantine", "fail"]},
                        },
                        "required": ["name", "condition", "action"],
                    },
                    "description": "Array of quality rule objects",
                },
            },
            "required": ["table_name", "rules"],
        },
    },
    {
        "name": "write_governance_sql",
        "description": (
            "Write Unity Catalog governance SQL (tags, column masks, row filters). "
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name",
                },
                "sql": {
                    "type": "string",
                    "description": "SQL statements for governance (ALTER TABLE SET TAGS, CREATE FUNCTION, etc.)",
                },
            },
            "required": ["table_name", "sql"],
        },
    },
    {
        "name": "write_validation_test",
        "description": (
            "Write a validation notebook that compares source and target data. "
            "Used to verify migration correctness."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pipeline_name": {
                    "type": "string",
                    "description": "Name of the pipeline being validated",
                },
                "source_table": {
                    "type": "string",
                    "description": "Source table reference (for comparison)",
                },
                "target_table": {
                    "type": "string",
                    "description": "Target table in Unity Catalog",
                },
                "validation_code": {
                    "type": "string",
                    "description": "Notebook code for validation checks",
                },
            },
            "required": ["pipeline_name", "target_table", "validation_code"],
        },
    },
    {
        "name": "analyze_complexity",
        "description": (
            "Analyze source code complexity to determine migration difficulty. "
            "Returns scores and findings. Use this BEFORE transforming code."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_code": {
                    "type": "string",
                    "description": "Source code to analyze",
                },
            },
            "required": ["source_code"],
        },
    },
    {
        "name": "map_cluster",
        "description": (
            "Map an EMR cluster configuration to Databricks cluster config. "
            "Returns recommended instance types, sizing, and Photon eligibility."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "emr_config": {
                    "type": "object",
                    "description": "EMR cluster configuration dict with InstanceType, InstanceCount, etc.",
                },
            },
            "required": ["emr_config"],
        },
    },
    {
        "name": "report_progress",
        "description": (
            "Report migration progress for a pipeline. "
            "Call this after completing each pipeline to track state."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pipeline_name": {
                    "type": "string",
                    "description": "Name of the pipeline",
                },
                "status": {
                    "type": "string",
                    "enum": ["completed", "failed", "needs_review"],
                    "description": "Migration status",
                },
                "artifacts": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of file paths written",
                },
                "notes": {
                    "type": "string",
                    "description": "Any notes or warnings for the reviewer",
                },
            },
            "required": ["pipeline_name", "status"],
        },
    },
]


@dataclass
class ToolResult:
    """Result of a tool execution."""

    success: bool
    output: str
    artifacts: list[str] = field(default_factory=list)


class AgentTools:
    """
    Executes tool calls from the Claude agent.

    Each tool performs a deterministic operation (write file, analyze code, etc.)
    and returns a structured result that Claude uses to decide next steps.
    """

    def __init__(self, output_dir: str, catalog: str = "production", org: str = "default"):
        self.output_dir = output_dir
        self.catalog = catalog
        self.org = org
        self.artifacts_written: list[str] = []
        self.progress: list[dict] = []

    def execute(self, tool_name: str, tool_input: dict) -> ToolResult:
        """Dispatch a tool call to the appropriate handler."""
        handlers = {
            "write_notebook": self._write_notebook,
            "write_job_yaml": self._write_job_yaml,
            "write_quality_rules": self._write_quality_rules,
            "write_governance_sql": self._write_governance_sql,
            "write_validation_test": self._write_validation_test,
            "analyze_complexity": self._analyze_complexity,
            "map_cluster": self._map_cluster,
            "report_progress": self._report_progress,
        }

        handler = handlers.get(tool_name)
        if not handler:
            return ToolResult(success=False, output=f"Unknown tool: {tool_name}")

        try:
            return handler(tool_input)
        except Exception as e:
            logger.error(f"Tool {tool_name} failed: {e}")
            return ToolResult(success=False, output=f"Error: {e}")

    def _write_notebook(self, params: dict) -> ToolResult:
        """Write a notebook file to the output directory."""
        import os

        path = params["path"]
        code = params["code"]

        # Sanitize path to prevent traversal
        safe_path = os.path.normpath(path).lstrip(os.sep).lstrip(".")
        if ".." in safe_path:
            return ToolResult(success=False, output="Path traversal detected — rejected.")

        full_path = os.path.join(self.output_dir, safe_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, "w", encoding="utf-8") as f:
            f.write(code)

        self.artifacts_written.append(safe_path)
        logger.info(f"Wrote notebook: {safe_path}")
        return ToolResult(
            success=True,
            output=f"Notebook written to {safe_path} ({len(code)} chars)",
            artifacts=[safe_path],
        )

    def _write_job_yaml(self, params: dict) -> ToolResult:
        """Write a job YAML file."""
        import os

        name = re.sub(r"[^a-z0-9_]", "_", params["name"].lower())
        rel_path = f"resources/jobs/{name}.yml"
        full_path = os.path.join(self.output_dir, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, "w", encoding="utf-8") as f:
            f.write(params["yaml_content"])

        self.artifacts_written.append(rel_path)
        logger.info(f"Wrote job YAML: {rel_path}")
        return ToolResult(success=True, output=f"Job YAML written to {rel_path}", artifacts=[rel_path])

    def _write_quality_rules(self, params: dict) -> ToolResult:
        """Write quality rules as SQL expectations."""
        import os

        table_name = params["table_name"]
        rules = params["rules"]
        safe_name = re.sub(r"[^a-z0-9_]", "_", table_name.split(".")[-1])
        rel_path = f"quality/{safe_name}_expectations.sql"
        full_path = os.path.join(self.output_dir, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        lines = [f"-- Data Quality Expectations for {table_name}", ""]
        for rule in rules:
            action = rule.get("action", "warn").upper()
            dq_action = "DROP ROW" if action in ("DROP", "QUARANTINE") else "FAIL UPDATE" if action == "FAIL" else "DROP ROW"
            lines.append(f"-- Rule: {rule['name']} (action: {action})")
            lines.append(f"-- EXPECT ({rule['condition']}) ON VIOLATION {dq_action}")
            lines.append("")

        with open(full_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        self.artifacts_written.append(rel_path)
        return ToolResult(success=True, output=f"Quality rules written to {rel_path}", artifacts=[rel_path])

    def _write_governance_sql(self, params: dict) -> ToolResult:
        """Write governance SQL."""
        import os

        table_name = params["table_name"]
        safe_name = re.sub(r"[^a-z0-9_]", "_", table_name.split(".")[-1])
        rel_path = f"governance/{safe_name}_governance.sql"
        full_path = os.path.join(self.output_dir, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, "w", encoding="utf-8") as f:
            f.write(f"-- Governance SQL for {table_name}\n\n")
            f.write(params["sql"])

        self.artifacts_written.append(rel_path)
        return ToolResult(success=True, output=f"Governance SQL written to {rel_path}", artifacts=[rel_path])

    def _write_validation_test(self, params: dict) -> ToolResult:
        """Write a validation test notebook."""
        import os

        name = re.sub(r"[^a-z0-9_]", "_", params["pipeline_name"].lower())
        rel_path = f"tests/validation/test_{name}.py"
        full_path = os.path.join(self.output_dir, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, "w", encoding="utf-8") as f:
            f.write(params["validation_code"])

        self.artifacts_written.append(rel_path)
        return ToolResult(success=True, output=f"Validation test written to {rel_path}", artifacts=[rel_path])

    def _analyze_complexity(self, params: dict) -> ToolResult:
        """Analyze source code complexity using the deterministic analyzer."""
        from aws2lakehouse.discovery.complexity_analyzer import ComplexityAnalyzer

        analyzer = ComplexityAnalyzer()
        result = analyzer.analyze_code(params["source_code"])

        return ToolResult(
            success=True,
            output=json.dumps({
                "score": result.overall_score,
                "category": result.category,
                "estimated_hours": result.estimated_hours,
                "findings": result.findings,
                "recommended_approach": result.recommended_approach,
                "has_streaming": result.has_streaming,
                "has_udfs": result.has_udfs,
                "has_jars": result.has_custom_jars,
                "has_hive": result.uses_hive_metastore,
            }, indent=2),
        )

    def _map_cluster(self, params: dict) -> ToolResult:
        """Map EMR cluster to Databricks config."""
        from aws2lakehouse.compute.cluster_mapper import ClusterMapper

        mapper = ClusterMapper()
        emr_config = params["emr_config"]
        result = mapper.map_emr_cluster(emr_config)

        return ToolResult(
            success=True,
            output=json.dumps({
                "node_type": result.node_type_id,
                "driver_node_type": result.driver_node_type_id,
                "num_workers": result.num_workers,
                "autoscale": result.autoscale,
                "spark_conf": result.spark_conf,
                "photon_enabled": result.photon_enabled,
            }, indent=2),
        )

    def _report_progress(self, params: dict) -> ToolResult:
        """Track migration progress."""
        entry = {
            "pipeline": params["pipeline_name"],
            "status": params["status"],
            "artifacts": params.get("artifacts", []),
            "notes": params.get("notes", ""),
        }
        self.progress.append(entry)
        logger.info(f"Progress: {params['pipeline_name']} → {params['status']}")
        return ToolResult(
            success=True,
            output=f"Recorded: {params['pipeline_name']} = {params['status']}",
        )
