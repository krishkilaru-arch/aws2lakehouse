"""
ai_client.py — Databricks Foundation Model API client for aws2lakehouse.

Uses databricks-claude-opus-4-6 (1M token context, advanced reasoning)
for intelligent migration tasks.
"""
import json
import os
from dataclasses import dataclass, field
from typing import Any, Optional

import requests


@dataclass
class ComparisonResult:
    """Result of a semantic comparison between source and target migration code."""
    pipeline_name: str
    verdict: str              # "PASS", "WARN", "FAIL"
    logic_parity: bool        # Core business logic preserved
    issues: list[dict[str, str]] = field(default_factory=list)  # [{severity, category, description, suggestion}]
    summary: str = ""         # Plain-English summary
    confidence: float = 0.0   # 0.0–1.0 confidence in the assessment

    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0

    def __str__(self) -> str:
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(self.verdict, "?")
        lines = [f"{icon} {self.pipeline_name}: {self.verdict} (confidence: {self.confidence:.0%})"]
        lines.append(f"   Logic parity: {'Yes' if self.logic_parity else 'NO'}")
        if self.issues:
            lines.append(f"   Issues ({len(self.issues)}):")
            for issue in self.issues:
                lines.append(f"     [{issue['severity']}] {issue['category']}: {issue['description']}")
                if issue.get('suggestion'):
                    lines.append(f"       → Fix: {issue['suggestion']}")
        if self.summary:
            lines.append(f"   Summary: {self.summary}")
        return "\n".join(lines)


class AIClient:
    """Databricks Foundation Model client for migration intelligence."""

    MODELS = {
        "opus":   "databricks-claude-opus-4-6",
        "sonnet": "databricks-claude-sonnet-4-5",
        "haiku":  "databricks-claude-haiku-4-5",
    }

    def __init__(self, model: str = "opus", max_tokens: int = 4096):
        self.model = self.MODELS.get(model, model)
        self.max_tokens = max_tokens

        # Auth: try env vars first, then SDK, then notebook context
        self.host = os.environ.get("DATABRICKS_HOST", "")
        token = os.environ.get("DATABRICKS_TOKEN", "")

        if not self.host or not token:
            try:
                from databricks.sdk import WorkspaceClient
                w = WorkspaceClient()
                self.host = w.config.host
                self._headers = {**w.config.authenticate(), "Content-Type": "application/json"}
                self._total_tokens = 0
                return
            except Exception:
                pass
            try:
                from dbruntime.databricks_repl_context import get_context
                ctx = get_context()
                self.host = f"https://{ctx.browserHostName}"
                token = ctx.apiToken
            except Exception:
                raise RuntimeError(
                    "Set DATABRICKS_HOST and DATABRICKS_TOKEN env vars, "
                    "or run inside a Databricks notebook/cluster."
                )

        self._headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self._total_tokens = 0

    def _call(self, messages: list[dict], temperature: float = 0.2,
              max_tokens: Optional[int] = None) -> str:
        """Make API call with retry logic and token validation."""
        import time

        # A2: Estimate input tokens and truncate if needed
        total_chars = sum(len(m.get("content", "")) for m in messages)
        max_input_chars = 800_000  # ~200K tokens safe limit for opus (1M ctx)
        if total_chars > max_input_chars:
            # Truncate the longest message (usually user content)
            excess = total_chars - max_input_chars
            for m in reversed(messages):
                if len(m.get("content", "")) > excess + 1000:
                    m["content"] = m["content"][:len(m["content"]) - excess] + "\n[TRUNCATED]"
                    break

        # A1: Retry with exponential backoff
        last_error = None
        for attempt in range(3):
            try:
                response = requests.post(
                    f"{self.host}/serving-endpoints/{self.model}/invocations",
                    headers=self._headers,
                    json={"messages": messages, "max_tokens": max_tokens or self.max_tokens,
                          "temperature": temperature},
                    timeout=120
                )
                if response.status_code == 429:
                    wait = 2 ** attempt * 5  # 5s, 10s, 20s
                    time.sleep(wait)
                    continue
                response.raise_for_status()

                content = response.json()["choices"][0]["message"]["content"]

                # Track usage
                usage = response.json().get("usage", {})
                self._total_tokens += usage.get("total_tokens", 0)

                return content
            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < 2:
                    time.sleep(2 ** attempt * 2)

        raise RuntimeError(f"AI call failed after 3 retries: {last_error}")

    @staticmethod
    def _strip_markdown(text: str) -> str:
        """A3: Strip markdown code fences from AI response."""
        import re
        # Remove ```python ... ``` or ```...``` wrapper
        stripped = re.sub(r'^```(?:python)?\s*\n', '', text.strip())
        stripped = re.sub(r'\n```\s*$', '', stripped)
        # Remove trailing explanation after code block
        if '\n```\n' in stripped:
            stripped = stripped[:stripped.rfind('\n```\n')]
        return stripped.strip()

    def transform_code(self, source_code: str, source_type: str = "emr", context: str = "") -> str:
        system = f"""You are an expert migrating {source_type.upper()} code to Databricks. Rules:
- Unity Catalog 3-level namespace (catalog.schema.table)
- Replace GlueContext/DynamicFrame with SparkSession/DataFrame
- Replace s3:// with Volumes or Auto Loader
- Delta format for all writes
- dbutils.secrets.get() for credentials
- Add _ingested_at audit column
{f"Context: {context}" if context else ""}
Return ONLY the transformed Python code with NO markdown fences or explanations."""
        result = self._call([
            {"role": "system", "content": system},
            {"role": "user", "content": f"Transform:\n```python\n{source_code}\n```"}
        ])
        return self._strip_markdown(result)

    def explain_pipeline(self, pipeline_spec: dict[str, Any]) -> str:
        system = """Write a concise pipeline description for business stakeholders.
Include: data flow, purpose, SLA, compliance. Under 150 words."""
        spec_str = json.dumps(pipeline_spec, indent=2) if isinstance(pipeline_spec, dict) else str(pipeline_spec)
        return self._call([
            {"role": "system", "content": system},
            {"role": "user", "content": f"Describe:\n{spec_str}"}
        ])

    def suggest_quality_rules(self, table_name: str, columns: list[str], domain: str) -> str:
        system = """Suggest DQ expectations for financial data. Return CONSTRAINT statements only."""
        return self._call([
            {"role": "system", "content": system},
            {"role": "user", "content": f"Table: {table_name}\nColumns: {', '.join(columns)}\nDomain: {domain}"}
        ])

    def debug_migration(self, error_message: str, source_code: str, context: str = "") -> str:
        system = """Diagnose migration error. Give: 1) Root cause, 2) Fix code, 3) Prevention tip."""
        return self._call([
            {"role": "system", "content": system},
            {"role": "user", "content": f"Error: {error_message}\nCode:\n```\n{source_code}\n```\nContext: {context}"}
        ])

    def generate_migration_summary(self, inventory: list[dict], source_desc: str, dest_desc: str) -> str:
        system = """Write a 200-word executive summary for a regulated financial services migration.
Focus on: what migrated, improvements gained, risks mitigated, next steps. Use bullets."""
        inv_summary = json.dumps([{"name": p["name"], "domain": p["domain"],
                                    "source_type": p["source_type"],
                                    "classification": p.get("classification", "internal")}
                                   for p in inventory], indent=2)
        return self._call([
            {"role": "system", "content": system},
            {"role": "user", "content": f"Source: {source_desc}\nDest: {dest_desc}\nPipelines ({len(inventory)}):\n{inv_summary}"}
        ])

    def enrich_docs(self, doc_content: str, context: str) -> str:
        system = """Enhance migration docs with risk callouts and recommendations as > blockquotes."""
        return self._call([
            {"role": "system", "content": system},
            {"role": "user", "content": f"Context: {context}\n\nDocument:\n{doc_content}"}
        ], max_tokens=8192)

    # ═══════════════════════════════════════════════════════════════════════
    # POST-MIGRATION SEMANTIC COMPARISON
    # ═══════════════════════════════════════════════════════════════════════

    def compare_migration(
        self,
        source_code: str,
        target_code: str,
        pipeline_name: str = "unknown",
        source_type: str = "emr",
        context: str = "",
    ) -> ComparisonResult:
        """Semantically compare AWS source code vs generated Databricks code.

        Checks:
          1. Logic parity — same filters, joins, aggregations, business rules
          2. Config drift — Spark configs that changed semantics
          3. Missing transforms — logic in source not present in target
          4. Schema changes — columns added/dropped/renamed/retyped
          5. Edge cases — NULL handling, timezone, type coercion, ordering

        Args:
            source_code: Original AWS code (EMR script, Glue job, Airflow task)
            target_code: Generated Databricks notebook/code
            pipeline_name: Pipeline identifier for the result
            source_type: "emr", "glue", "airflow", "step_function"
            context: Additional context (domain, classification, merge keys)

        Returns:
            ComparisonResult with verdict, issues, and suggestions.
        """
        system = f"""You are a senior data engineer reviewing a migration from AWS {source_type.upper()} to Databricks.

Compare the SOURCE (AWS) and TARGET (Databricks) code. Identify semantic differences that could cause data or behavior drift.

CHECK THESE CATEGORIES:
1. logic_parity — Are filters, joins, aggregations, CASE/WHEN, window functions identical in intent?
2. config_drift — Did any Spark config change affect shuffle partitions, broadcast thresholds, or write semantics?
3. missing_transforms — Is there business logic in the source that is NOT in the target?
4. schema_changes — Were columns added, dropped, renamed, or retyped without explicit handling?
5. edge_cases — NULL handling, timezone conversions, type coercions, ordering assumptions, dedup logic

{f"Context: {context}" if context else ""}

Respond with ONLY valid JSON (no markdown fences) in this exact structure:
{{
  "verdict": "PASS" | "WARN" | "FAIL",
  "logic_parity": true | false,
  "confidence": 0.0 to 1.0,
  "summary": "one-sentence overall assessment",
  "issues": [
    {{
      "severity": "critical" | "high" | "medium" | "low",
      "category": "logic_parity" | "config_drift" | "missing_transforms" | "schema_changes" | "edge_cases",
      "description": "what is wrong",
      "suggestion": "how to fix it"
    }}
  ]
}}

If codes are semantically equivalent, return verdict=PASS with empty issues array.
Be precise — do NOT flag cosmetic differences (variable names, comments, import order)."""

        user_msg = f"""SOURCE ({source_type.upper()}):
```
{source_code}
```

TARGET (Databricks):
```
{target_code}
```"""

        raw = self._call(
            [{"role": "system", "content": system}, {"role": "user", "content": user_msg}],
            temperature=0.1,
            max_tokens=4096,
        )

        # Parse JSON response (strip fences if AI adds them despite instructions)
        raw = self._strip_markdown(raw)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Fallback: AI didn't return valid JSON
            return ComparisonResult(
                pipeline_name=pipeline_name,
                verdict="WARN",
                logic_parity=True,
                summary=f"AI returned non-JSON response: {raw[:200]}",
                confidence=0.0,
                issues=[{"severity": "low", "category": "edge_cases",
                         "description": "AI response could not be parsed as JSON",
                         "suggestion": "Re-run comparison or review manually"}],
            )

        return ComparisonResult(
            pipeline_name=pipeline_name,
            verdict=data.get("verdict", "WARN"),
            logic_parity=data.get("logic_parity", True),
            confidence=data.get("confidence", 0.5),
            summary=data.get("summary", ""),
            issues=data.get("issues", []),
        )

    def batch_compare(
        self,
        file_pairs: list[dict[str, str]],
        source_type: str = "emr",
        context: str = "",
        threads: int = 1,
    ) -> list[ComparisonResult]:
        """Compare multiple source→target file pairs.

        Args:
            file_pairs: List of dicts with keys:
                - "name": pipeline name
                - "source_path": path to AWS source file
                - "target_path": path to generated Databricks file
            source_type: Source platform type
            context: Shared context for all comparisons
            threads: Parallel threads (1 = sequential)

        Returns:
            List of ComparisonResult objects.
        """
        from pathlib import Path

        def _compare_one(pair: dict[str, str]) -> ComparisonResult:
            source = Path(pair["source_path"]).read_text()
            target = Path(pair["target_path"]).read_text()
            return self.compare_migration(
                source_code=source,
                target_code=target,
                pipeline_name=pair.get("name", Path(pair["source_path"]).stem),
                source_type=source_type,
                context=context,
            )

        if threads <= 1:
            return [_compare_one(p) for p in file_pairs]

        from concurrent.futures import ThreadPoolExecutor, as_completed
        results = []
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(_compare_one, p): p for p in file_pairs}
            for future in as_completed(futures):
                results.append(future.result())

        # Sort by original order
        name_order = {p.get("name", ""): i for i, p in enumerate(file_pairs)}
        results.sort(key=lambda r: name_order.get(r.pipeline_name, 999))
        return results

    def print_comparison_report(self, results: list[ComparisonResult]) -> str:
        """Generate a formatted comparison report from batch results."""
        lines = []
        lines.append("═" * 70)
        lines.append("  POST-MIGRATION SEMANTIC COMPARISON REPORT")
        lines.append("═" * 70)
        lines.append("")

        passed = sum(1 for r in results if r.verdict == "PASS")
        warned = sum(1 for r in results if r.verdict == "WARN")
        failed = sum(1 for r in results if r.verdict == "FAIL")
        total_issues = sum(len(r.issues) for r in results)

        lines.append(f"  Pipelines compared: {len(results)}")
        lines.append(f"  ✅ PASS: {passed}  ⚠️  WARN: {warned}  ❌ FAIL: {failed}")
        lines.append(f"  Total issues found: {total_issues}")
        lines.append(f"  Tokens used: {self._total_tokens:,}")
        lines.append("")
        lines.append("─" * 70)

        for result in results:
            lines.append(str(result))
            lines.append("")

        lines.append("═" * 70)

        if failed > 0:
            lines.append("  ⚠️  ACTION REQUIRED: Fix FAIL items before production cutover.")
        elif warned > 0:
            lines.append("  ℹ️  Review WARN items — may be intentional migration decisions.")
        else:
            lines.append("  ✅ All pipelines pass semantic comparison. Safe to proceed.")

        lines.append("═" * 70)

        report = "\n".join(lines)
        print(report)
        return report

    @property
    def total_tokens(self) -> int:
        """Total tokens consumed across all API calls in this session."""
        return self._total_tokens
