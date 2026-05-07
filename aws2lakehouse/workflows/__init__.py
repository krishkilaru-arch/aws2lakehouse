"""
Orchestration Templates — Reusable Databricks Workflow patterns.

Pre-built workflow templates for common migration patterns:
- Multi-task DAG (Bronze → Silver → Gold with parallel branches)
- Conditional execution (IF/ELSE based on data quality)
- For-each pattern (process multiple tables with same logic)
- Medallion pipeline (standard ingestion → transform → aggregate)
- Backfill workflow (historical data reprocessing)
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class WorkflowTask:
    """Task in a Databricks Workflow."""
    task_key: str
    notebook_path: str
    depends_on: List[str] = field(default_factory=list)
    parameters: Dict[str, str] = field(default_factory=dict)
    condition: Optional[str] = None  # run_if condition
    cluster_type: str = "serverless"  # serverless | job_cluster | existing
    max_retries: int = 2
    timeout_seconds: int = 3600


class WorkflowTemplates:
    """Pre-built Databricks Workflow YAML templates."""
    
    @staticmethod
    def medallion_pipeline(name: str, domain: str, tables: List[str],
                           catalog: str = "production",
                           schedule: str = "0 6 * * *") -> str:
        """
        Standard Medallion Pipeline: Bronze → Silver → Gold for multiple tables.
        
        Generates a multi-task workflow where:
        - Bronze tasks run in parallel (one per source table)
        - Silver tasks depend on their Bronze counterpart
        - Gold tasks depend on ALL Silver tasks completing
        """
        bronze_tasks = []
        silver_tasks = []
        
        for table in tables:
            bronze_tasks.append(f"""        - task_key: bronze_{table}
          notebook_task:
            notebook_path: /pipelines/{domain}/bronze/{table}
            base_parameters:
              CATALOG: "{catalog}"
              TABLE: "{table}"
          max_retries: 2
          timeout_seconds: 1800""")
            
            silver_tasks.append(f"""        - task_key: silver_{table}
          notebook_task:
            notebook_path: /pipelines/{domain}/silver/{table}
            base_parameters:
              CATALOG: "{catalog}"
              TABLE: "{table}"
          depends_on:
            - task_key: bronze_{table}
          max_retries: 1
          timeout_seconds: 3600""")
        
        gold_deps = "\n".join([f"            - task_key: silver_{t}" for t in tables])
        
        return f"""# Medallion Pipeline: {domain}
resources:
  jobs:
    {name}:
      name: "[prod] {domain}/medallion_pipeline"
      schedule:
        quartz_cron_expression: "0 {schedule.split()[0]} {schedule.split()[1]} ? * * *"
        timezone_id: "America/New_York"
      
      tasks:
        # ═══ BRONZE LAYER (parallel ingestion) ═══
{chr(10).join(bronze_tasks)}
        
        # ═══ SILVER LAYER (sequential per table) ═══
{chr(10).join(silver_tasks)}
        
        # ═══ GOLD LAYER (aggregation after all silver) ═══
        - task_key: gold_aggregation
          notebook_task:
            notebook_path: /pipelines/{domain}/gold/aggregation
            base_parameters:
              CATALOG: "{catalog}"
              DOMAIN: "{domain}"
          depends_on:
{gold_deps}
          max_retries: 1
          timeout_seconds: 7200
        
        # ═══ QUALITY CHECK ═══
        - task_key: quality_validation
          notebook_task:
            notebook_path: /pipelines/{domain}/quality/validate_all
          depends_on:
            - task_key: gold_aggregation
      
      tags:
        domain: "{domain}"
        pattern: "medallion"
"""

    @staticmethod
    def conditional_workflow(name: str, domain: str,
                           check_notebook: str,
                           success_notebook: str,
                           failure_notebook: str) -> str:
        """
        Conditional Workflow: Run different paths based on a condition.
        
        Pattern: check → IF success THEN process ELSE remediate
        Uses run_if/condition_task for branching.
        """
        return f"""# Conditional Workflow: {name}
resources:
  jobs:
    {name}:
      name: "[prod] {domain}/{name}"
      tasks:
        # Step 1: Check condition (e.g., data quality gate)
        - task_key: check_condition
          notebook_task:
            notebook_path: {check_notebook}
          max_retries: 0
        
        # Step 2a: Happy path (condition passed)
        - task_key: process_data
          notebook_task:
            notebook_path: {success_notebook}
          depends_on:
            - task_key: check_condition
          # Only run if check succeeded
          condition_task:
            op: EQUAL_TO
            left: "{{{{tasks.check_condition.result_state}}}}"
            right: "SUCCESS"
        
        # Step 2b: Remediation path (condition failed)
        - task_key: remediate
          notebook_task:
            notebook_path: {failure_notebook}
          depends_on:
            - task_key: check_condition
          # Only run if check failed
          condition_task:
            op: EQUAL_TO
            left: "{{{{tasks.check_condition.result_state}}}}"
            right: "FAILED"
        
        # Step 3: Always notify
        - task_key: notify
          notebook_task:
            notebook_path: /common/notify_completion
          depends_on:
            - task_key: process_data
              outcome: SUCCEEDED
            - task_key: remediate
              outcome: SUCCEEDED
"""

    @staticmethod
    def for_each_workflow(name: str, domain: str,
                         tables: List[str],
                         notebook_path: str,
                         schedule: str = "0 6 * * *") -> str:
        """
        For-Each Workflow: Process multiple items with the same logic.
        
        Pattern: Same notebook runs for each table/entity, parameterized.
        Uses for_each_task pattern.
        """
        return f"""# For-Each Workflow: {name}
resources:
  jobs:
    {name}:
      name: "[prod] {domain}/{name}"
      schedule:
        quartz_cron_expression: "0 {schedule.split()[0]} {schedule.split()[1]} ? * * *"
        timezone_id: "UTC"
      
      tasks:
        - task_key: process_all_tables
          for_each_task:
            inputs: "[{', '.join([chr(34) + t + chr(34) for t in tables])}]"
            task:
              task_key: process_table
              notebook_task:
                notebook_path: {notebook_path}
                base_parameters:
                  TABLE: "{{{{input}}}}"
                  DOMAIN: "{domain}"
          max_retries: 1
          timeout_seconds: 7200
"""

    @staticmethod
    def backfill_workflow(name: str, table: str,
                         start_date: str, end_date: str,
                         notebook_path: str,
                         parallelism: int = 5) -> str:
        """
        Backfill Workflow: Reprocess historical data in date-partitioned chunks.
        
        Generates date ranges and processes them with controlled parallelism.
        """
        return f"""# Backfill Workflow: {name}
# Reprocesses {table} from {start_date} to {end_date}
resources:
  jobs:
    {name}:
      name: "[prod] backfill/{table}"
      max_concurrent_runs: 1
      
      tasks:
        - task_key: generate_date_ranges
          notebook_task:
            notebook_path: /common/generate_date_ranges
            base_parameters:
              START_DATE: "{start_date}"
              END_DATE: "{end_date}"
              CHUNK_DAYS: "7"
        
        - task_key: backfill_chunks
          depends_on:
            - task_key: generate_date_ranges
          for_each_task:
            inputs: "{{{{tasks.generate_date_ranges.values.date_ranges}}}}"
            concurrency: {parallelism}
            task:
              task_key: process_chunk
              notebook_task:
                notebook_path: {notebook_path}
                base_parameters:
                  TABLE: "{table}"
                  START: "{{{{input.start}}}}"
                  END: "{{{{input.end}}}}"
                  MODE: "backfill"
          max_retries: 2
        
        - task_key: validate_backfill
          depends_on:
            - task_key: backfill_chunks
          notebook_task:
            notebook_path: /common/validate_backfill
            base_parameters:
              TABLE: "{table}"
              START_DATE: "{start_date}"
              END_DATE: "{end_date}"
"""

    @staticmethod
    def dual_write_cutover(name: str, domain: str,
                           legacy_notebook: str,
                           new_notebook: str,
                           validation_notebook: str) -> str:
        """
        Dual-Write Cutover: Run both old and new pipelines, compare results.
        
        Pattern for zero-downtime migration validation:
        1. Run legacy pipeline (continues producing)
        2. Run new pipeline (shadow mode)
        3. Compare outputs
        4. Alert if differences found
        """
        return f"""# Dual-Write Cutover: {name}
# Both pipelines run in parallel for validation
resources:
  jobs:
    {name}_dual_write:
      name: "[prod] cutover/{name}_dual_write"
      tasks:
        # Run BOTH pipelines in parallel
        - task_key: legacy_pipeline
          notebook_task:
            notebook_path: {legacy_notebook}
        
        - task_key: new_pipeline
          notebook_task:
            notebook_path: {new_notebook}
        
        # Compare outputs after both complete
        - task_key: validate_match
          notebook_task:
            notebook_path: {validation_notebook}
            base_parameters:
              LEGACY_TABLE: "{domain}_legacy.output"
              NEW_TABLE: "{domain}_new.output"
              TOLERANCE: "0.001"
          depends_on:
            - task_key: legacy_pipeline
            - task_key: new_pipeline
        
        # Alert if mismatch
        - task_key: alert_on_mismatch
          notebook_task:
            notebook_path: /common/alert_mismatch
          depends_on:
            - task_key: validate_match
          condition_task:
            op: EQUAL_TO
            left: "{{{{tasks.validate_match.result_state}}}}"
            right: "FAILED"
"""


class FolderStructureGenerator:
    """Generate standardized project folder structure."""
    
    @staticmethod
    def generate(org: str, domains: List[str], environments: List[str] = None) -> str:
        """Generate the recommended folder structure as a tree."""
        environments = environments or ["dev", "staging", "production"]
        
        tree = f"""# Recommended Project Structure for {org}
#
# /Workspace/Repos/{org}-data-platform/
# ├── databricks.yml                    # DAB bundle configuration
# ├── resources/                        # Job/workflow definitions
# │   ├── jobs/
# │   │   ├── risk_medallion.yml
# │   │   ├── lending_ingestion.yml
# │   │   └── compliance_reports.yml
# │   └── clusters/
# │       ├── shared_analytics.yml
# │       └── streaming_always_on.yml
# ├── src/
# │   ├── pipelines/                    # Pipeline notebooks (per domain)
"""
        for domain in domains:
            tree += f"""# │   │   ├── {domain}/
# │   │   │   ├── bronze/              # Ingestion notebooks
# │   │   │   ├── silver/              # Transformation notebooks
# │   │   │   ├── gold/                # Aggregation notebooks
# │   │   │   └── quality/             # DQ validation notebooks
"""
        tree += f"""# │   ├── common/                      # Shared utilities
# │   │   ├── notify_completion.py
# │   │   ├── validate_backfill.py
# │   │   └── generate_date_ranges.py
# │   └── governance/                   # Governance SQL scripts
# │       ├── setup_catalogs.sql
# │       ├── column_masks.sql
# │       └── row_filters.sql
# ├── tests/                            # Test suites
# │   ├── unit/
# │   ├── integration/
# │   └── validation/
# ├── specs/                            # Pipeline YAML specs
# │   ├── risk/
# │   ├── lending/
# │   └── compliance/
# ├── docs/                             # Documentation
# └── .github/
#     └── workflows/
#         └── deploy.yml
"""
        return tree
    
    @staticmethod
    def generate_setup_script(org: str, domains: List[str]) -> str:
        """Generate a shell script to create the folder structure."""
        lines = ["#!/bin/bash", f"# Setup folder structure for {org}", ""]
        dirs = [
            "resources/jobs", "resources/clusters",
            "src/common", "src/governance",
            "tests/unit", "tests/integration", "tests/validation",
            "docs", ".github/workflows"
        ]
        for domain in domains:
            dirs.extend([
                f"src/pipelines/{domain}/bronze",
                f"src/pipelines/{domain}/silver",
                f"src/pipelines/{domain}/gold",
                f"src/pipelines/{domain}/quality",
                f"specs/{domain}",
            ])
        
        for d in sorted(dirs):
            lines.append(f"mkdir -p {d}")
        
        lines.append("")
        lines.append("echo '✅ Project structure created'")
        return "\n".join(lines)
