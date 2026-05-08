# Databricks notebook source
# MAGIC %md
# MAGIC # AWS → Databricks Migration Agent
# MAGIC
# MAGIC **Powered by Claude** — This notebook uses an AI agent to migrate AWS pipelines
# MAGIC to Databricks with full code transformation, quality rules, and documentation.
# MAGIC
# MAGIC ## Setup
# MAGIC 1. Install: `%pip install aws2lakehouse[agent]`
# MAGIC 2. Set up Claude access (either Anthropic API key or Databricks Model Serving)
# MAGIC 3. Point to your source repo
# MAGIC 4. Run!

# COMMAND ----------

# MAGIC %pip install aws2lakehouse[agent] --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Migration configuration
CONFIG = {
    "catalog": "production",          # Target Unity Catalog
    "org": "acme",                    # Organization name
    "source_repo_path": "/Workspace/Repos/migration/aws-source",  # Source code location
    "output_path": "/Workspace/Repos/migration/databricks-output", # Output location
    "domain": "trading",              # Business domain
}

# Claude configuration — OPTION 1: Direct Anthropic API
# (Set ANTHROPIC_API_KEY as a Databricks secret)
CLAUDE_CONFIG = {
    "api_key": dbutils.secrets.get(scope="migration", key="anthropic_api_key"),
    "model": "claude-sonnet-4-20250514",
}

# OPTION 2: Databricks Model Serving (if you've registered Claude as external model)
# CLAUDE_CONFIG = {
#     "databricks_host": spark.conf.get("spark.databricks.workspaceUrl"),
#     "databricks_token": dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
#     "model": "claude-sonnet-endpoint",  # Your serving endpoint name
# }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Scan Source Repository

# COMMAND ----------

import os
import sys

# Add source repo to path for scanning
sys.path.insert(0, "/Workspace/Repos/migration/aws2lakehouse")
from migrate import scan_source, build_inventory

# Scan the AWS source repository
source_path = CONFIG["source_repo_path"]
manifest = scan_source(source_path)

print(f"📁 Source: {source_path}")
print(f"   Airflow DAGs: {len(manifest['airflow_dags'])}")
print(f"   EMR Scripts:  {len(manifest['emr_scripts'])}")
print(f"   Glue Jobs:    {len(manifest['glue_jobs'])}")
print(f"   Step Functions: {len(manifest['step_functions'])}")
print(f"   Configs:      {len(manifest['configs'])}")

# COMMAND ----------

# Build inventory with metadata
inventory = build_inventory(manifest, source_path)
print(f"\n📊 Inventory: {len(inventory)} pipelines")

# Display as table
import pandas as pd
df = pd.DataFrame(inventory)[["name", "domain", "source_type", "classification", "schedule"]]
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Initialize Migration Agent

# COMMAND ----------

from aws2lakehouse.agent import MigrationAgent

agent = MigrationAgent(
    catalog=CONFIG["catalog"],
    org=CONFIG["org"],
    output_dir=CONFIG["output_path"],
    **CLAUDE_CONFIG,
)

print(f"✅ Agent initialized")
print(f"   Model: {CLAUDE_CONFIG.get('model', 'claude-sonnet-4-20250514')}")
print(f"   Output: {CONFIG['output_path']}")
print(f"   Catalog: {CONFIG['catalog']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Migrate Pipelines (Agent Loop)
# MAGIC
# MAGIC The agent will:
# MAGIC 1. Analyze each pipeline's complexity
# MAGIC 2. Transform the code to Databricks PySpark
# MAGIC 3. Generate Workflow job YAML
# MAGIC 4. Infer data quality rules
# MAGIC 5. Write validation tests

# COMMAND ----------

# Prepare pipelines for agent
pipelines_for_agent = []

for item in inventory:
    # Read the source file
    source_path = item.get("source_path", "")
    if source_path and os.path.exists(source_path):
        with open(source_path, encoding="utf-8", errors="replace") as f:
            source_code = f.read()
    else:
        continue

    pipelines_for_agent.append({
        "name": item["name"],
        "source_code": source_code,
        "source_type": item.get("source_type", "glue"),
        "domain": item.get("domain", CONFIG["domain"]),
        "layer": "bronze",
        "context": f"Schedule: {item.get('schedule', 'daily')}. "
                   f"Classification: {item.get('classification', 'internal')}.",
    })

print(f"📦 Prepared {len(pipelines_for_agent)} pipelines for migration")

# COMMAND ----------

# Run the agent on all pipelines
batch_result = agent.migrate_batch(pipelines_for_agent)

print(f"\n{'='*60}")
print(f"🏁 {batch_result.summary}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Review Results

# COMMAND ----------

# Summary table
results_data = []
for r in batch_result.results:
    results_data.append({
        "Pipeline": r.pipeline_name,
        "Status": r.status,
        "Artifacts": len(r.artifacts),
        "Warnings": len(r.warnings),
        "Tokens": r.token_usage.get("total_tokens", 0),
        "Duration (s)": round(r.duration_seconds, 1),
    })

display(pd.DataFrame(results_data))

# COMMAND ----------

# Show any pipelines that need review
needs_review = [r for r in batch_result.results if r.status == "needs_review"]
if needs_review:
    print(f"⚠️  {len(needs_review)} pipelines need manual review:")
    for r in needs_review:
        print(f"   • {r.pipeline_name}: {'; '.join(r.warnings)}")
else:
    print("✅ All pipelines migrated successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Semantic Validation
# MAGIC
# MAGIC Use the agent to compare source and target for behavioral drift.

# COMMAND ----------

# Validate first completed pipeline
completed = [r for r in batch_result.results if r.status == "completed" and r.notebook_code]
if completed:
    first = completed[0]
    source = pipelines_for_agent[0]["source_code"]

    print(f"🔍 Validating: {first.pipeline_name}")
    diff_report = agent.compare_migration(
        source_code=source,
        target_code=first.notebook_code,
        source_type=pipelines_for_agent[0]["source_type"],
    )
    print(diff_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Deploy (Optional)
# MAGIC
# MAGIC The output directory now contains a complete Databricks Asset Bundle.
# MAGIC Deploy with:
# MAGIC ```bash
# MAGIC cd /Workspace/Repos/migration/databricks-output
# MAGIC databricks bundle deploy --target staging
# MAGIC ```

# COMMAND ----------

# List generated artifacts
output_path = CONFIG["output_path"]
if os.path.exists(output_path):
    for root, dirs, files in os.walk(output_path):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), output_path)
            print(f"  📄 {rel}")
else:
    print(f"Output directory not yet created: {output_path}")
    print("Run the migration first (Step 3)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Summary

# COMMAND ----------

print(f"""
💰 Migration Cost Summary
{'='*40}
Total pipelines:    {batch_result.total}
Total tokens used:  {batch_result.total_tokens:,}
Estimated cost:     ${batch_result.total_tokens * 0.000003:.2f} (input) + ${batch_result.total_tokens * 0.000015:.2f} (output)
Total duration:     {batch_result.total_duration_seconds:.0f}s
Avg per pipeline:   {batch_result.total_duration_seconds / max(batch_result.total, 1):.1f}s

Compare to manual: ~{batch_result.total * 16}h engineer time @ $150/h = ${batch_result.total * 16 * 150:,.0f}
""")

