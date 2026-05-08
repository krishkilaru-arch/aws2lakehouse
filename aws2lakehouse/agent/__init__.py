"""
Agent Layer — Claude-powered migration agent for Databricks.

This module provides an agentic migration system that uses Claude to:
1. Understand and transform source code (Glue, EMR, Airflow → Databricks)
2. Convert stored procedures (Redshift SQL → Spark SQL)
3. Generate context-aware quality rules from data profiles
4. Write human-quality migration documentation
5. Validate migration correctness via semantic comparison

Architecture:
    ┌─────────────────────────────────────────────────────┐
    │  DETERMINISTIC LAYER (scan, inventory, scaffold)    │
    │  • scan_source() / build_inventory()                │
    │  • ClusterMapper / PipelineFactory                  │
    │  • State management / resume                        │
    └──────────────────────┬──────────────────────────────┘
                           │
    ┌──────────────────────▼──────────────────────────────┐
    │  AGENT LAYER (this module — Claude-powered)         │
    │  • Code understanding & transformation              │
    │  • Business logic extraction                        │
    │  • Stored procedure → Spark SQL conversion          │
    │  • Quality rule inference                           │
    │  • Semantic diff (source vs target behavior)        │
    └─────────────────────────────────────────────────────┘

Usage (Databricks notebook):
    from aws2lakehouse.agent import MigrationAgent

    agent = MigrationAgent(catalog="production", org="acme")
    result = agent.migrate_pipeline(
        source_code=open("glue_jobs/etl_trades.py").read(),
        source_type="glue",
        domain="trading",
    )
    # result.notebook_code — transformed PySpark notebook
    # result.job_yaml — Databricks Workflow definition
    # result.quality_rules — inferred DQ expectations
    # result.documentation — migration notes

Usage (CLI):
    aws2lakehouse agent --source ./aws-repo --catalog production --org acme
"""

from aws2lakehouse.agent.orchestrator import MigrationAgent, MigrationResult
from aws2lakehouse.agent.tools import AgentTools

__all__ = [
    "MigrationAgent",
    "MigrationResult",
    "AgentTools",
]
