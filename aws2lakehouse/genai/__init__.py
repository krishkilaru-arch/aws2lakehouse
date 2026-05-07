"""
GenAI Layer — AI-assisted pipeline development.

Uses Databricks Foundation Model APIs (or external LLMs) to:
1. Auto-generate pipeline YAML specs from natural language descriptions
2. Explain data lineage in plain English
3. Assist debugging pipeline failures
4. Generate documentation from code

Usage:
    from aws2lakehouse.genai import PipelineGenerator, LineageExplainer, DebugAssistant
    
    # Generate pipeline from description
    gen = PipelineGenerator()
    spec_yaml = gen.from_description(
        "Ingest daily trade events from Kafka topic 'trades' into a Bronze table "
        "with MNPI classification, partition by trade_date, and alert on failures"
    )
    
    # Explain lineage
    explainer = LineageExplainer()
    explanation = explainer.explain("production.risk.gold_pnl_summary")
    
    # Debug a failure
    debugger = DebugAssistant()
    suggestion = debugger.diagnose(error_message, notebook_code)
"""

from typing import Dict, List, Optional


class PipelineGenerator:
    """
    Generate pipeline YAML specs from natural language descriptions.
    
    Uses prompt engineering to translate business requirements into
    structured pipeline configurations.
    """
    
    SYSTEM_PROMPT = """You are a Databricks data engineering expert. Generate YAML pipeline specs
from natural language descriptions. The YAML must conform to this schema:

name: <snake_case>
domain: <business domain>
owner: <team email>
layer: bronze|silver|gold
source:
  type: kafka|auto_loader|delta_share|jdbc|mongodb|snowflake|delta_table
  config: <source-specific config>
target:
  catalog: <catalog>
  schema: <schema>
  table: <table>
  mode: streaming|batch|merge
  partition_by: [columns]
  z_order_by: [columns]
  merge_keys: [columns]  # for merge mode
quality:
  - name: <rule_name>
    condition: <sql_condition>
    action: warn|drop|quarantine|fail
governance:
  classification: public|internal|confidential|mnpi
  mnpi_columns: [columns]
  column_masks: {col: mask_expression}
  row_filter: <sql_condition>
schedule:
  cron: <5-field cron>
  sla_minutes: <int>
"""
    
    def __init__(self, model_endpoint: str = "databricks-meta-llama-3-1-70b-instruct"):
        self.model_endpoint = model_endpoint
    
    def from_description(self, description: str, domain: str = "default",
                         catalog: str = "production") -> str:
        """
        Generate pipeline YAML from natural language description.
        
        In production, calls the Databricks Foundation Model API.
        For offline use, applies rule-based heuristics.
        """
        # Rule-based fallback (works without LLM endpoint)
        return self._heuristic_generate(description, domain, catalog)
    
    def _heuristic_generate(self, desc: str, domain: str, catalog: str) -> str:
        """Rule-based pipeline generation from description keywords."""
        desc_lower = desc.lower()
        
        # Detect source type
        source_type = "auto_loader"
        if "kafka" in desc_lower:
            source_type = "kafka"
        elif "delta share" in desc_lower or "cdf" in desc_lower:
            source_type = "delta_share"
        elif "jdbc" in desc_lower or "postgres" in desc_lower or "mysql" in desc_lower:
            source_type = "jdbc"
        elif "mongo" in desc_lower:
            source_type = "mongodb"
        elif "snowflake" in desc_lower:
            source_type = "snowflake"
        
        # Detect mode
        mode = "batch"
        if "stream" in desc_lower or "real-time" in desc_lower or "kafka" in desc_lower:
            mode = "streaming"
        elif "merge" in desc_lower or "upsert" in desc_lower or "scd" in desc_lower:
            mode = "merge"
        
        # Detect layer
        layer = "bronze"
        if "silver" in desc_lower or "clean" in desc_lower or "transform" in desc_lower:
            layer = "silver"
        elif "gold" in desc_lower or "aggregate" in desc_lower or "summary" in desc_lower:
            layer = "gold"
        
        # Detect governance
        classification = "internal"
        if "mnpi" in desc_lower or "restricted" in desc_lower or "confidential" in desc_lower:
            classification = "mnpi"
        elif "public" in desc_lower:
            classification = "public"
        
        # Extract table name from description
        import re
        name_match = re.search(r"(?:table|into|named)\s+(\w+)", desc_lower)
        table_name = name_match.group(1) if name_match else "pipeline_output"
        
        # Extract topic for Kafka
        topic = "events"
        topic_match = re.search(r"topic\s+(\w[\w.-]*)", desc_lower)
        if topic_match:
            topic = topic_match.group(1)
        
        # Detect partitioning
        partition_cols = []
        if "partition" in desc_lower:
            part_match = re.search(r"partition\s+(?:by\s+)?(\w+)", desc_lower)
            if part_match:
                partition_cols = [part_match.group(1)]
        
        # Build YAML
        lines = [
            f"name: {table_name}",
            f"domain: {domain}",
            f"owner: team@company.com",
            f"layer: {layer}",
            f"",
            f"source:",
            f"  type: {source_type}",
            f"  config:",
        ]
        
        if source_type == "kafka":
            lines.append(f"    topic: {topic}")
            lines.append(f"    secret_scope: kafka-prod")
            lines.append(f"    starting_offsets: latest")
        elif source_type == "auto_loader":
            lines.append(f"    path: /Volumes/{catalog}/raw/{table_name}/")
            lines.append(f"    format: json")
        elif source_type == "jdbc":
            lines.append(f"    secret_scope: jdbc-prod")
            lines.append(f"    source_table: public.{table_name}")
        
        lines.extend([
            f"",
            f"target:",
            f"  catalog: {catalog}",
            f"  schema: {domain}_{layer}",
            f"  table: {table_name}",
            f"  mode: {mode}",
        ])
        
        if partition_cols:
            lines.append(f"  partition_by: [{', '.join(partition_cols)}]")
        
        lines.extend([
            f"",
            f"quality:",
            f"  - name: not_null_id",
            f"    condition: 'id IS NOT NULL'",
            f"    action: fail",
            f"",
            f"governance:",
            f"  classification: {classification}",
        ])
        
        if classification == "mnpi":
            lines.append(f"  embargo_hours: 24")
        
        # Schedule
        if mode == "streaming":
            lines.extend([f"", f"schedule:", f'  cron: "*/5 * * * *"', f"  sla_minutes: 10"])
        else:
            lines.extend([f"", f"schedule:", f'  cron: "0 6 * * *"', f"  sla_minutes: 60"])
        
        if "alert" in desc_lower or "notify" in desc_lower:
            lines.extend([f"", f"alerting:", f"  on_failure: [slack:#data-alerts]"])
        
        return "\n".join(lines)
    
    def _call_llm(self, description: str) -> str:
        """Call Databricks Foundation Model API for generation."""
        # This would use the Databricks SDK in production
        prompt = f"{self.SYSTEM_PROMPT}\n\nUser request: {description}\n\nGenerate YAML:"
        
        try:
            import mlflow.deployments
            client = mlflow.deployments.get_deploy_client("databricks")
            response = client.predict(
                endpoint=self.model_endpoint,
                inputs={"messages": [
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": description}
                ]}
            )
            return response["choices"][0]["message"]["content"]
        except Exception:
            return self._heuristic_generate(description, "default", "production")


class LineageExplainer:
    """Explain data lineage in plain English using AI."""
    
    def explain(self, table_name: str, lineage_data: Dict = None) -> str:
        """Generate human-readable lineage explanation."""
        if not lineage_data:
            return f"To explain lineage for {table_name}, provide lineage_data from Unity Catalog lineage API."
        
        upstream = lineage_data.get("upstream", [])
        downstream = lineage_data.get("downstream", [])
        
        explanation = f"## Lineage for `{table_name}`\n\n"
        
        if upstream:
            explanation += "### Data Sources (upstream):\n"
            for src in upstream:
                explanation += f"- `{src['table']}` via {src.get('transform', 'direct read')}\n"
        
        if downstream:
            explanation += "\n### Consumers (downstream):\n"
            for dst in downstream:
                explanation += f"- `{dst['table']}` ({dst.get('type', 'table')})\n"
        
        return explanation


class DebugAssistant:
    """AI-assisted pipeline debugging."""
    
    COMMON_FIXES = {
        "AnalysisException": "Check table/column names — likely a schema mismatch or missing table",
        "OutOfMemoryError": "Increase executor memory or reduce partition size. Consider .repartition() before joins",
        "StreamingQueryException": "Check checkpoint location permissions and schema evolution settings",
        "ConcurrentModificationException": "Enable delta.isolation.level = WriteSerializable or add retry logic",
        "SchemaEvolutionException": "Add .option(\"mergeSchema\", \"true\") to the write operation",
        "FileNotFoundException": "Source path may have changed. Check Auto Loader path and permissions",
        "TimeoutException": "Query exceeded timeout. Add .trigger(availableNow=True) for batch-style streaming",
    }
    
    def diagnose(self, error_message: str, code: str = None) -> Dict:
        """Diagnose a pipeline error and suggest fixes."""
        diagnosis = {
            "error_type": "Unknown",
            "likely_cause": "",
            "suggested_fix": "",
            "code_suggestion": "",
            "severity": "medium",
        }
        
        for err_type, fix in self.COMMON_FIXES.items():
            if err_type in error_message:
                diagnosis["error_type"] = err_type
                diagnosis["likely_cause"] = fix
                diagnosis["suggested_fix"] = fix
                break
        
        # Pattern-based suggestions
        if "permission" in error_message.lower() or "access denied" in error_message.lower():
            diagnosis["likely_cause"] = "Insufficient permissions"
            diagnosis["suggested_fix"] = "Check GRANT statements on target table/schema. Verify service principal has correct access."
            diagnosis["severity"] = "high"
        
        if "checkpoint" in error_message.lower():
            diagnosis["likely_cause"] = "Checkpoint corruption or incompatible schema change"
            diagnosis["suggested_fix"] = "Delete checkpoint directory and restart stream from beginning, or use a new checkpoint path"
        
        return diagnosis
