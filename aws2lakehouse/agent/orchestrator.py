"""
Migration Agent Orchestrator — The brain of the agent-based migration.

This module implements the agentic loop that:
1. Receives a pipeline (source code + metadata)
2. Sends it to Claude with migration-specific tools
3. Processes tool calls (write files, analyze, etc.)
4. Loops until Claude signals completion
5. Returns structured results

Works in:
- Databricks notebooks (interactive or job)
- Local development (with ANTHROPIC_API_KEY)
- CI/CD pipelines (headless batch mode)
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field

from aws2lakehouse.agent.prompts import (
    CODE_TRANSFORM_PROMPT,
    DOCUMENTATION_PROMPT,
    QUALITY_INFERENCE_PROMPT,
    SEMANTIC_DIFF_PROMPT,
    SQL_TRANSFORM_PROMPT,
    SYSTEM_PROMPT,
)
from aws2lakehouse.agent.tools import TOOL_DEFINITIONS, AgentTools

logger = logging.getLogger(__name__)


@dataclass
class MigrationResult:
    """Result of migrating a single pipeline."""

    pipeline_name: str
    status: str  # "completed" | "failed" | "needs_review"
    artifacts: list[str] = field(default_factory=list)
    notebook_code: str = ""
    job_yaml: str = ""
    quality_rules: list[dict] = field(default_factory=list)
    documentation: str = ""
    warnings: list[str] = field(default_factory=list)
    token_usage: dict = field(default_factory=dict)
    duration_seconds: float = 0.0


@dataclass
class BatchMigrationResult:
    """Result of migrating multiple pipelines."""

    total: int = 0
    completed: int = 0
    failed: int = 0
    needs_review: int = 0
    results: list[MigrationResult] = field(default_factory=list)
    total_tokens: int = 0
    total_duration_seconds: float = 0.0

    @property
    def summary(self) -> str:
        return (
            f"Migration complete: {self.completed}/{self.total} succeeded, "
            f"{self.failed} failed, {self.needs_review} need review. "
            f"Tokens used: {self.total_tokens:,}. "
            f"Duration: {self.total_duration_seconds:.1f}s"
        )


class MigrationAgent:
    """
    Claude-powered migration agent.

    Uses Claude's tool_use capability to transform AWS data pipelines
    into Databricks artifacts. The agent decides which tools to call
    and in what order based on the source code context.

    Args:
        catalog: Target Unity Catalog name (e.g., "production")
        org: Organization name for naming conventions
        output_dir: Directory to write generated artifacts
        model: Claude model to use (default: claude-sonnet-4-20250514)
        max_tokens: Maximum tokens per response
        api_key: Anthropic API key (or set ANTHROPIC_API_KEY env var)
        databricks_host: If set, uses Databricks Model Serving endpoint
        databricks_token: Token for Databricks Model Serving

    Example:
        agent = MigrationAgent(catalog="production", org="acme")
        result = agent.migrate_pipeline(
            source_code=code,
            source_type="glue",
            domain="trading",
        )
    """

    def __init__(
        self,
        catalog: str = "production",
        org: str = "default",
        output_dir: str = "./output",
        model: str = "claude-sonnet-4-20250514",
        max_tokens: int = 8192,
        api_key: str | None = None,
        databricks_host: str | None = None,
        databricks_token: str | None = None,
    ):
        self.catalog = catalog
        self.org = org
        self.output_dir = output_dir
        self.model = model
        self.max_tokens = max_tokens
        self.tools = AgentTools(output_dir=output_dir, catalog=catalog, org=org)

        # Initialize the appropriate client
        self._client = self._init_client(api_key, databricks_host, databricks_token)

    def _init_client(self, api_key, databricks_host, databricks_token):
        """Initialize Claude client — either direct Anthropic or via Databricks."""
        if databricks_host and databricks_token:
            # Use Databricks External Model Serving (proxied Claude)
            return _DatabricksClaudeClient(
                host=databricks_host,
                token=databricks_token,
                model=self.model,
            )

        # Direct Anthropic API
        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Provide api_key, set ANTHROPIC_API_KEY env var, "
                "or pass databricks_host/databricks_token for Model Serving."
            )
        return _AnthropicClient(api_key=resolved_key, model=self.model)

    # ─── Public API ─────────────────────────────────────────────────────────

    def migrate_pipeline(
        self,
        source_code: str,
        source_type: str = "glue",
        domain: str = "default",
        layer: str = "bronze",
        schema: str | None = None,
        pipeline_name: str | None = None,
        additional_context: str = "",
    ) -> MigrationResult:
        """
        Migrate a single pipeline using Claude agent.

        Args:
            source_code: The AWS source code to migrate
            source_type: One of "glue", "emr", "airflow", "step_function", "redshift"
            domain: Business domain (e.g., "trading", "risk", "lending")
            layer: Target layer ("bronze", "silver", "gold")
            schema: Target schema (defaults to f"{domain}_{layer}")
            pipeline_name: Name for the pipeline (auto-inferred if not provided)
            additional_context: Extra context for the agent (e.g., "this reads from Kafka")

        Returns:
            MigrationResult with all generated artifacts
        """
        start = time.time()
        schema = schema or f"{domain}_{layer}"
        pipeline_name = pipeline_name or self._infer_name(source_code, source_type)

        # Build the user message
        if source_type in ("redshift", "sql"):
            user_msg = SQL_TRANSFORM_PROMPT.format(
                source_type=source_type,
                source_sql=source_code,
                catalog=self.catalog,
                schema=schema,
            )
        else:
            user_msg = CODE_TRANSFORM_PROMPT.format(
                source_type=source_type,
                source_code=source_code,
                catalog=self.catalog,
                schema=schema,
                domain=domain,
                layer=layer,
            )

        if additional_context:
            user_msg += f"\n\n## Additional Context\n{additional_context}"

        # Add instruction to use tools
        user_msg += (
            "\n\n## Instructions\n"
            "1. First call analyze_complexity to assess the source code\n"
            "2. Then write_notebook with the transformed code\n"
            "3. Then write_job_yaml with the workflow definition\n"
            "4. Then write_quality_rules with inferred expectations\n"
            "5. Finally call report_progress to record completion\n"
        )

        # Run the agent loop
        messages = [{"role": "user", "content": user_msg}]
        total_tokens = 0
        result = MigrationResult(pipeline_name=pipeline_name, status="in_progress")

        for iteration in range(10):  # Max 10 agent turns
            response = self._client.create_message(
                system=SYSTEM_PROMPT,
                messages=messages,
                tools=TOOL_DEFINITIONS,
                max_tokens=self.max_tokens,
            )

            total_tokens += response.get("usage", {}).get("input_tokens", 0)
            total_tokens += response.get("usage", {}).get("output_tokens", 0)

            # Process response content
            assistant_content = response.get("content", [])
            messages.append({"role": "assistant", "content": assistant_content})

            # Check if agent is done (no more tool calls)
            tool_calls = [b for b in assistant_content if b.get("type") == "tool_use"]
            if not tool_calls:
                # Agent responded with text only — extract any final notes
                text_blocks = [b.get("text", "") for b in assistant_content if b.get("type") == "text"]
                if text_blocks:
                    result.documentation += "\n".join(text_blocks)
                break

            # Execute each tool call
            tool_results = []
            for tool_call in tool_calls:
                tool_name = tool_call["name"]
                tool_input = tool_call["input"]
                tool_id = tool_call["id"]

                logger.info(f"  [{iteration}] Tool: {tool_name}")
                exec_result = self.tools.execute(tool_name, tool_input)

                # Track artifacts
                result.artifacts.extend(exec_result.artifacts)

                # Capture specific outputs
                if tool_name == "write_notebook":
                    result.notebook_code = tool_input.get("code", "")
                elif tool_name == "write_job_yaml":
                    result.job_yaml = tool_input.get("yaml_content", "")
                elif tool_name == "write_quality_rules":
                    result.quality_rules = tool_input.get("rules", [])
                elif tool_name == "report_progress":
                    result.status = tool_input.get("status", "completed")
                    if tool_input.get("notes"):
                        result.warnings.append(tool_input["notes"])

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "content": exec_result.output,
                })

            messages.append({"role": "user", "content": tool_results})

        # Finalize
        if result.status == "in_progress":
            result.status = "completed"

        result.token_usage = {"total_tokens": total_tokens}
        result.duration_seconds = time.time() - start

        logger.info(
            f"Pipeline '{pipeline_name}' migrated: status={result.status}, "
            f"artifacts={len(result.artifacts)}, tokens={total_tokens}"
        )
        return result

    def migrate_batch(
        self,
        pipelines: list[dict],
        resume_from: str | None = None,
    ) -> BatchMigrationResult:
        """
        Migrate multiple pipelines in sequence.

        Args:
            pipelines: List of dicts with keys: source_code, source_type, domain, name
            resume_from: Skip pipelines until this name is reached

        Returns:
            BatchMigrationResult with per-pipeline results
        """
        batch = BatchMigrationResult(total=len(pipelines))
        start = time.time()
        skip = resume_from is not None

        for pipeline in pipelines:
            name = pipeline.get("name", "unnamed")

            if skip:
                if name == resume_from:
                    skip = False
                else:
                    logger.info(f"Skipping {name} (resuming from {resume_from})")
                    continue

            logger.info(f"Migrating [{batch.completed + batch.failed + 1}/{batch.total}]: {name}")

            try:
                result = self.migrate_pipeline(
                    source_code=pipeline["source_code"],
                    source_type=pipeline.get("source_type", "glue"),
                    domain=pipeline.get("domain", "default"),
                    layer=pipeline.get("layer", "bronze"),
                    pipeline_name=name,
                    additional_context=pipeline.get("context", ""),
                )
                batch.results.append(result)
                batch.total_tokens += result.token_usage.get("total_tokens", 0)

                if result.status == "completed":
                    batch.completed += 1
                elif result.status == "failed":
                    batch.failed += 1
                else:
                    batch.needs_review += 1

            except Exception as e:
                logger.error(f"Pipeline '{name}' failed: {e}")
                batch.results.append(MigrationResult(
                    pipeline_name=name,
                    status="failed",
                    warnings=[str(e)],
                ))
                batch.failed += 1

        batch.total_duration_seconds = time.time() - start
        logger.info(batch.summary)
        return batch

    def compare_migration(
        self,
        source_code: str,
        target_code: str,
        source_type: str = "glue",
    ) -> str:
        """
        Semantic comparison of source and target code.

        Returns a structured analysis of any behavioral differences.
        """
        user_msg = SEMANTIC_DIFF_PROMPT.format(
            source_type=source_type,
            source_code=source_code,
            target_code=target_code,
        )

        response = self._client.create_message(
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
            tools=[],
            max_tokens=4096,
        )

        text_blocks = [b.get("text", "") for b in response.get("content", []) if b.get("type") == "text"]
        return "\n".join(text_blocks)

    def infer_quality_rules(self, code: str, table_name: str, columns: list[str] | None = None) -> list[dict]:
        """
        Infer data quality rules from pipeline code.

        Returns a list of rule dicts: [{name, condition, action, description}]
        """
        user_msg = QUALITY_INFERENCE_PROMPT.format(
            code=code,
            table_name=table_name,
            columns=", ".join(columns) if columns else "(infer from code)",
        )

        response = self._client.create_message(
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
            tools=[],
            max_tokens=4096,
        )

        text_blocks = [b.get("text", "") for b in response.get("content", []) if b.get("type") == "text"]
        text = "\n".join(text_blocks)

        # Extract JSON from response
        try:
            # Find JSON array in response
            match = json.loads(text) if text.strip().startswith("[") else []
            if not match:
                import re
                json_match = re.search(r"\[.*\]", text, re.DOTALL)
                if json_match:
                    match = json.loads(json_match.group())
            return match
        except (json.JSONDecodeError, TypeError):
            logger.warning("Could not parse quality rules from agent response")
            return []

    def generate_documentation(
        self,
        pipeline_name: str,
        source_type: str,
        domain: str,
        source_code: str,
        target_code: str,
        changes: str = "",
    ) -> str:
        """Generate migration documentation for a pipeline."""
        user_msg = DOCUMENTATION_PROMPT.format(
            pipeline_name=pipeline_name,
            source_type=source_type,
            domain=domain,
            source_code=source_code[:3000],  # Truncate to fit context
            target_code=target_code[:3000],
            changes=changes or "(agent-determined)",
        )

        response = self._client.create_message(
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
            tools=[],
            max_tokens=4096,
        )

        text_blocks = [b.get("text", "") for b in response.get("content", []) if b.get("type") == "text"]
        return "\n".join(text_blocks)

    # ─── Private Helpers ────────────────────────────────────────────────────

    def _infer_name(self, code: str, source_type: str) -> str:
        """Infer a pipeline name from code content."""
        import re

        # Try to find DAG ID, job name, function name
        patterns = [
            r"dag_id\s*=\s*['\"]([^'\"]+)['\"]",
            r"job\.setJobName\(['\"]([^'\"]+)",
            r"def\s+(main|process|run|etl)\s*\(",
            r"table_name\s*=\s*['\"]([^'\"]+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, code)
            if m:
                return re.sub(r"[^a-z0-9_]", "_", m.group(1).lower())

        return f"migrated_{source_type}_pipeline"


# ─── Client Implementations ─────────────────────────────────────────────────


class _AnthropicClient:
    """Direct Anthropic API client."""

    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model

    def create_message(self, system: str, messages: list, tools: list, max_tokens: int) -> dict:
        """Call Claude via the Anthropic Python SDK."""
        try:
            import anthropic
        except ImportError as err:
            raise ImportError(
                "The 'anthropic' package is required. Install with: pip install anthropic"
            ) from err

        client = anthropic.Anthropic(api_key=self.api_key)

        kwargs = {
            "model": self.model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
        }
        if tools:
            kwargs["tools"] = tools

        response = client.messages.create(**kwargs)

        # Convert to dict format
        content = []
        for block in response.content:
            if block.type == "text":
                content.append({"type": "text", "text": block.text})
            elif block.type == "tool_use":
                content.append({
                    "type": "tool_use",
                    "id": block.id,
                    "name": block.name,
                    "input": block.input,
                })

        return {
            "content": content,
            "usage": {
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            },
            "stop_reason": response.stop_reason,
        }


class _DatabricksClaudeClient:
    """Claude via Databricks External Model Serving endpoint."""

    def __init__(self, host: str, token: str, model: str):
        self.host = host.rstrip("/")
        self.token = token
        self.model = model

    def create_message(self, system: str, messages: list, tools: list, max_tokens: int) -> dict:
        """Call Claude via Databricks Model Serving (external model)."""
        import requests

        endpoint_url = f"{self.host}/serving-endpoints/{self.model}/invocations"

        payload = {
            "anthropic_version": "2023-06-01",
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
        }
        if tools:
            payload["tools"] = tools

        resp = requests.post(
            endpoint_url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=120,
        )

        if resp.status_code != 200:
            raise RuntimeError(f"Databricks Model Serving error ({resp.status_code}): {resp.text[:500]}")

        data = resp.json()

        # Databricks proxied response is already in Anthropic format
        return {
            "content": data.get("content", []),
            "usage": data.get("usage", {}),
            "stop_reason": data.get("stop_reason", "end_turn"),
        }
