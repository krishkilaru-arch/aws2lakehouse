# Migration Playbook — End-to-End Guide

## Overview

This playbook guides you through migrating 1000+ AWS data pipelines to Databricks
using the aws2lakehouse accelerator. The methodology is designed for regulated
financial services environments with MNPI controls, compliance requirements,
and zero-tolerance for data loss.

**Timeline:** 6 months (can be compressed to 4 with dedicated team)  
**Team:** 2-3 senior Databricks architects + customer engineering team  
**Outcome:** All pipelines on Databricks with governance, monitoring, and CI/CD

---

## Phase 1: Discovery & Assessment (Weeks 1-3)

### Goal
Build a complete inventory of all data pipelines, classify them, and create
a migration roadmap that stakeholders approve.

### Step 1.1: Scan AWS Infrastructure

```python
from aws2lakehouse.discovery.pipeline_inventory import PipelineInventory

# Initialize scanner
inventory = PipelineInventory(regions=["us-east-1", "us-west-2"])

# Scan all pipeline sources
inventory.scan_emr_clusters(region="us-east-1")
inventory.scan_glue_jobs(region="us-east-1")
inventory.scan_step_functions(region="us-east-1")

# Generate assessment report
report = inventory.generate_assessment_report()
print(f"Total pipelines: {report['total_pipelines']}")
print(f"By type: {report['by_type']}")
print(f"By complexity: {report['by_complexity']}")
```

### Step 1.2: Scan Additional Sources (if applicable)

```python
from aws2lakehouse.discovery import (
    AirflowScanner, SnowflakeScanner, MongoDBScanner, PostgreSQLScanner
)

# Airflow (provide DAG folder path or API URL)
airflow = AirflowScanner(dag_folder="/path/to/dags/")
dags = airflow.scan_dag_folder()

# Snowflake (generates SQL to run in Snowflake console)
sf = SnowflakeScanner()
print(sf.generate_scan_sql())  # Run this in Snowflake, import results

# MongoDB (generates shell script)
mongo = MongoDBScanner()
print(mongo.generate_scan_script("production_db"))

# PostgreSQL (generates SQL)
pg = PostgreSQLScanner()
print(pg.generate_scan_sql())  # Run in psql, import results
```

### Step 1.3: Score Pipeline Complexity

```python
from aws2lakehouse.discovery.complexity_analyzer import ComplexityAnalyzer

analyzer = ComplexityAnalyzer()

# For each pipeline's source code:
for pipeline in inventory.pipelines:
    # Read the pipeline code (from S3, Git, etc.)
    code = read_pipeline_code(pipeline.source_path)
    
    score = analyzer.analyze_code(code, {
        "sla_hours": pipeline.sla_hours,
        "schedule": pipeline.schedule,
        "has_dependencies": len(pipeline.dependencies) > 0,
    })
    
    pipeline.complexity_score = score.overall_score
    pipeline.complexity_category = score.category
    pipeline.recommended_approach = score.recommended_approach
    pipeline.estimated_hours = score.estimated_hours
```

**Scoring Guide:**
| Score | Category | Approach | Typical Effort |
|-------|----------|----------|----------------|
| 0-25 | Simple | Lift-and-shift | 2-4 hours |
| 26-50 | Medium | Minor refactor | 4-16 hours |
| 51-75 | Complex | Major refactor | 16-40 hours |
| 76-100 | Critical | Rewrite | 40-80 hours |

### Step 1.4: Classify Latency & MNPI

```python
from aws2lakehouse.discovery import LatencyClassifier

for pipeline in inventory.pipelines:
    pipeline.latency = LatencyClassifier.classify(
        schedule=pipeline.schedule,
        trigger_type=pipeline.trigger_type
    )
    # Returns: "batch", "nrt", or "streaming"
```

For MNPI classification, work with compliance team to tag each pipeline:
- **MNPI:** Handles material non-public information (earnings, trades, portfolio)
- **Confidential:** Internal sensitive data (PII, HR, compensation)
- **Internal:** Business data without regulatory restrictions
- **Public:** External-facing, no restrictions

### Step 1.5: Generate Migration Waves

```python
from aws2lakehouse.discovery.wave_planner import WavePlanner

planner = WavePlanner(inventory)
waves = planner.generate_waves(
    strategy="risk_first",    # Pilot low-risk first, then escalate
    max_per_wave=50,          # Manageable batch size
    pilot_count=3,            # 3 representative pipelines for pilot
)

for wave in waves:
    print(f"Wave {wave.wave_number}: {wave.name}")
    print(f"  Pipelines: {len(wave.pipelines)}")
    print(f"  Duration: {wave.estimated_duration_weeks} weeks")
    print(f"  Risk: {wave.risk_level}")
```

### Step 1.6: Calculate ROI (for stakeholder approval)

```python
from aws2lakehouse.roi_calculator import ROICalculator, AWSCurrentState, DatabricksProjectedState

current = AWSCurrentState(
    emr_clusters=50,
    emr_monthly_cost=120_000,
    glue_jobs=200,
    glue_monthly_cost=35_000,
    s3_monthly_cost=25_000,
    other_monthly_cost=30_000,
    engineers_maintaining=12,
    avg_engineer_monthly_cost=18_000,
)

projected = DatabricksProjectedState(
    jobs_dbu_monthly=55_000,
    sql_dbu_monthly=15_000,
    serverless_monthly=10_000,
    storage_monthly_cost=18_000,
    engineers_needed=7,
    avg_engineer_monthly_cost=18_000,
)

roi = ROICalculator(current, projected, migration_cost=850_000, migration_months=6)
print(roi.generate_executive_summary())
```

### Deliverables
- [ ] Pipeline inventory spreadsheet (name, type, complexity, domain, SLA)
- [ ] Migration wave roadmap (approved by stakeholders)
- [ ] ROI business case (signed off by finance)
- [ ] Risk register with mitigations

---

## Phase 2: Pilot Implementation (Weeks 4-6)

### Goal
Migrate 3 representative pipelines (simple, medium, complex) to validate
patterns and build team confidence.

### Step 2.1: Select Pilot Pipelines

Choose 3 pipelines that cover:
1. **Simple** — File ingestion (Auto Loader → Bronze)
2. **Medium** — Streaming + merge (Kafka → Bronze → Silver with SCD)
3. **Complex** — Multi-step orchestration + MNPI controls

Use the example specs as starting points:
```bash
examples/pilot/simple_pipeline.yaml    # Auto Loader → Bronze
examples/pilot/complex_pipeline.yaml   # Kafka + MNPI + column masks
```

### Step 2.2: Create Pipeline YAML Specs

```yaml
# pipelines/pilot/daily_transactions.yaml
name: daily_transactions
domain: finance
owner: data-eng@company.com
layer: bronze

source:
  type: auto_loader
  config:
    path: /Volumes/production/raw/transactions/
    format: csv

target:
  catalog: production
  schema: finance_bronze
  table: daily_transactions
  mode: streaming
  partition_by: [transaction_date]
  z_order_by: [account_id]

quality:
  - name: valid_id
    condition: "transaction_id IS NOT NULL"
    action: fail
  - name: valid_amount
    condition: "amount > 0"
    action: drop

governance:
  classification: internal

schedule:
  cron: "0 6 * * *"
  sla_minutes: 60

alerting:
  on_failure: [slack:#data-alerts]
```

### Step 2.3: Generate and Deploy Artifacts

```python
from aws2lakehouse.factory import PipelineSpec, PipelineFactory

spec = PipelineSpec.from_yaml("pipelines/pilot/daily_transactions.yaml")
factory = PipelineFactory(catalog="production", environment="dev")  # Start in dev!
artifacts = factory.generate(spec)

# Deploy notebook to workspace
# Deploy job via DAB
# Run governance SQL to set tags/masks
# Set up monitoring with generated SQL
```

### Step 2.4: Validate Results

Run the generated test suite:
```python
# artifacts.test_code contains pytest functions:
# - test_table_exists()
# - test_no_null_keys()
# - test_freshness()
# - test_dq_<rule_name>() for each quality rule
```

Compare results with source system:
- Row counts match (within tolerance)
- Schema matches
- Business logic produces same outputs
- SLA is met

### Deliverables
- [ ] 3 pilot pipelines running in dev
- [ ] Validation report (source vs target comparison)
- [ ] Performance benchmarks (EMR time vs Databricks time)
- [ ] Patterns refined based on pilot learnings

---

## Phase 3: Foundation Setup (Weeks 4-8, parallel with pilot)

### Goal
Establish the platform foundation: Unity Catalog, governance, CI/CD, monitoring.

### Step 3.1: Unity Catalog Setup

```python
from aws2lakehouse.governance import UnityCatalogSetup

uc = UnityCatalogSetup(
    org="apex",
    environments=["dev", "staging", "production"],
    domains=["lending", "risk", "customer", "compliance", "analytics"]
)

# Generate and execute setup SQL
setup_sql = uc.generate_setup_sql()
grants_sql = uc.generate_grants_sql()
volume_sql = uc.generate_volume_sql([
    {"name": "apex-raw-data", "environment": "production"},
    {"name": "apex-curated-data", "environment": "production"},
])

# Execute in Databricks SQL:
# spark.sql(setup_sql)  # Creates catalogs, schemas
# spark.sql(grants_sql) # Sets up RBAC
# spark.sql(volume_sql) # Creates external volumes
```

**Naming Convention:**
```
{org}_{env}.{domain}_{layer}.{table}
  apex_production.risk_bronze.trade_events
  apex_production.risk_silver.trades_cleaned
  apex_production.risk_gold.daily_pnl
```

### Step 3.2: MNPI Controls (Financial Services)

```python
from aws2lakehouse.governance import MNPIController

mnpi = MNPIController(catalog="apex_production")

# 1. Column masking
masks_sql = mnpi.generate_column_mask_sql([
    {
        "table": "apex_production.risk_bronze.trade_events",
        "columns": [
            {"name": "notional_amount", "data_type": "DECIMAL(18,2)",
             "mask_expression": "0.00", "approved_group": "risk_trading_desk"},
            {"name": "counterparty_name", "data_type": "STRING",
             "mask_expression": "'[RESTRICTED]'", "approved_group": "risk_trading_desk"},
            {"name": "pnl", "data_type": "DECIMAL(18,2)",
             "mask_expression": "NULL", "approved_group": "risk_mgmt"},
        ]
    }
])

# 2. Dynamic views (for consumers who shouldn't see raw tables)
views_sql = mnpi.generate_dynamic_views_sql([
    {
        "source_table": "apex_production.risk_bronze.trade_events",
        "view_name": "apex_production.risk_bronze.v_trade_events_safe",
        "row_filter": "trade_date < current_date() - INTERVAL 1 DAY",
        "column_rules": [
            {"name": "pnl", "default_expr": "NULL",
             "allowed_groups": ["risk_mgmt", "compliance"]},
        ]
    }
])

# 3. Audit logging
audit_sql = mnpi.generate_audit_table_sql()

# 4. Row-level filters (embargo periods)
filter_sql = mnpi.generate_row_filter_sql("apex_production.risk_gold.earnings_forecast")
```

### Step 3.3: CI/CD Pipeline

```python
from aws2lakehouse.cicd import DABGenerator, BranchStrategy

# Generate Databricks Asset Bundle configuration
dab = DABGenerator(
    project_name="apex-data-platform",
    environments=["dev", "staging", "production"],
    catalog_prefix="apex"
)

bundle_yaml = dab.generate_bundle_yaml()        # databricks.yml
github_actions = dab.generate_github_actions()   # .github/workflows/deploy.yml

# Branch strategy
codeowners = BranchStrategy.generate_codeowners()

# For Azure DevOps shops:
from aws2lakehouse.cicd.azure_devops import AzureDevOpsPipeline
azure_pipeline = AzureDevOpsPipeline.generate("apex-data-platform")
```

**Deployment Flow:**
```
feature/* → develop (staging) → main (production)
     │           │                    │
     ▼           ▼                    ▼
  validate    deploy-staging     deploy-production
  (lint+test)  (auto)             (manual approval)
```

### Step 3.4: Monitoring & Observability

```python
from aws2lakehouse.observability import MonitoringDashboard
from aws2lakehouse.quality import FreshnessSLAMonitor, VolumeAnomalyDetector

# Create monitoring schema and views
dashboard = MonitoringDashboard(catalog="apex_production", schema="observability")
monitoring_sql = dashboard.generate_all_sql()
# Execute: creates 7 views for pipeline health tracking

# Register tables for freshness monitoring
freshness = FreshnessSLAMonitor(catalog="apex_production")
freshness.add_table("apex_production.risk_bronze.trade_events", sla_minutes=15, owner="risk-team")
freshness.add_table("apex_production.lending_bronze.applications", sla_minutes=60, owner="lending-team")
freshness_view_sql = freshness.generate_monitoring_view_sql()

# Register tables for volume anomaly detection
volume = VolumeAnomalyDetector(catalog="apex_production")
volume.add_table("apex_production.risk_bronze.trade_events", low_threshold=0.5, high_threshold=2.0)
volume_view_sql = volume.generate_anomaly_view_sql()
```

### Deliverables
- [ ] Unity Catalog configured (all envs, all domains)
- [ ] MNPI column masks active on sensitive tables
- [ ] CI/CD pipeline deploying to dev/staging/prod
- [ ] Monitoring dashboard live with SLA tracking

---

## Phase 4: Migration Execution (Weeks 7-20)

### Goal
Migrate all pipelines in waves using the config-driven factory.

### Step 4.1: Create YAML Specs (per wave)

For each pipeline in the current wave, create a YAML spec. Use the factory
to auto-generate the initial spec from existing code:

```python
from aws2lakehouse.genai import PipelineGenerator

gen = PipelineGenerator()

# For each pipeline, describe it and generate YAML
for pipeline in current_wave.pipelines:
    description = f"Migrate {pipeline.name}: {pipeline.description}"
    yaml_spec = gen.from_description(description)
    
    # Save and then MANUALLY REVIEW/REFINE
    with open(f"pipelines/{pipeline.domain}/{pipeline.name}.yaml", "w") as f:
        f.write(yaml_spec)
```

**Important:** Always review generated specs. The GenAI layer provides a starting
point, but human review ensures correctness.

### Step 4.2: Batch-Generate Artifacts

```python
from aws2lakehouse.factory import PipelineFactory
from pathlib import Path

factory = PipelineFactory(catalog="apex_production", environment="prod")

# Generate ALL artifacts for the wave
artifacts_list = factory.generate_batch("pipelines/risk/")

for artifacts in artifacts_list:
    spec = artifacts.spec
    base = Path(f"output/{spec.domain}/{spec.name}")
    base.mkdir(parents=True, exist_ok=True)
    
    (base / "notebook.py").write_text(artifacts.notebook_code)
    (base / "job.yml").write_text(artifacts.job_yaml)
    (base / "dq_expectations.sql").write_text(artifacts.dq_sql)
    (base / "governance.sql").write_text(artifacts.governance_sql)
    (base / "tests.py").write_text(artifacts.test_code)
    (base / "monitoring.sql").write_text(artifacts.monitoring_sql)
```

### Step 4.3: EMR Job Migration (for lift-and-shift)

For pipelines classified as "lift-and-shift":

```python
from aws2lakehouse.emr.emr_migrator import EMRMigrator, SparkSubmitParser, JARAnalyzer

# Parse spark-submit command
parser = SparkSubmitParser()
config = parser.parse_command(pipeline.spark_submit_cmd)

# Analyze JARs
jar_analyzer = JARAnalyzer()
jar_results = jar_analyzer.analyze_jars(config.jars)
print(f"Auto-resolved: {len(jar_results['auto_resolved'])}")
print(f"Needs rewrite: {len(jar_results['needs_rewrite'])}")

# Transform code
migrator = EMRMigrator(target_catalog="apex_production")
transformed_code, warnings = migrator._transform_code(original_code)
# Handles: S3→Volumes, Hive→Unity Catalog, removes EMRFS configs
```

### Step 4.4: Cluster Mapping

```python
from aws2lakehouse.compute.cluster_mapper import ClusterMapper

mapper = ClusterMapper(photon_enabled=True)

# Map each EMR cluster to optimal Databricks compute
config = mapper.map_emr_cluster(
    instance_type="r5.2xlarge",
    instance_count=10,
    workload_type="batch_etl",      # batch_etl, sql_analytics, ml_training, streaming
    has_autoscaling=True,
    min_instances=4,
    max_instances=20,
)

print(f"Node type: {config.node_type_id}")
print(f"Workers: {config.num_workers}")
print(f"Compute type: {config.compute_type}")  # serverless, job_cluster, shared
print(f"Autoscale: {config.autoscale_min}-{config.autoscale_max}")
```

### Step 4.5: Orchestration Conversion

**Step Functions:**
```python
from aws2lakehouse.orchestration.step_function_converter import StepFunctionConverter
import json

with open("state_machines/daily_risk.json") as f:
    definition = json.load(f)

converter = StepFunctionConverter(target_notebook_base="/Workspace/pipelines/risk/")
job = converter.convert(definition, name="daily_risk_pipeline",
                       schedule_expression="cron(0 6 * * ? *)")

print(f"Tasks: {len(job.tasks)}")
for task in job.tasks:
    print(f"  {task.task_key} → {task.notebook_path}")
    if task.depends_on:
        print(f"    depends_on: {task.depends_on}")
```

**Airflow DAGs:**
```python
from aws2lakehouse.orchestration.airflow_converter import AirflowConverter

converter = AirflowConverter(target_notebook_base="/Workspace/pipelines/")
job = converter.convert_dag_file("dags/daily_etl.py")

print(f"DAG: {job.name}")
print(f"Tasks: {len(job.tasks)}")
print(f"Schedule: {job.schedule}")

# Review warnings
for warning in job.warnings:
    print(f"  ⚠️  {warning}")

# Generate the job YAML
print(job.to_yaml())
```

### Step 4.6: Validate Each Wave

For each migrated pipeline:
1. Run in dev with sample data
2. Compare outputs with source (row counts, checksums)
3. Run generated test suite
4. Verify SLA compliance
5. Check governance (masks active, tags applied)
6. Promote to staging → production

### Deliverables (per wave)
- [ ] All pipeline specs reviewed and finalized
- [ ] Artifacts generated and committed to Git
- [ ] Dev validation passed (source-target comparison)
- [ ] Staging deployment successful
- [ ] Production cutover (with rollback plan)

---

## Phase 5: Optimization & Hardening (Weeks 18-22)

### Step 5.1: Performance Tuning

```python
from aws2lakehouse.observability import PartitionStrategy

# Analyze query patterns and recommend optimization
recommendation = PartitionStrategy.recommend(
    table_size_gb=250,
    query_patterns=[
        "WHERE trade_date = '2024-01-15'",
        "WHERE account_id = 'ACC-12345'",
        "WHERE currency = 'USD'",
    ],
    cardinality={
        "trade_date": 365,
        "account_id": 50000,
        "currency": 7,
    }
)

print(f"Partition by: {recommendation['partition_by']}")
print(f"Z-ORDER by:   {recommendation['z_order_by']}")
print(f"Reasoning:     {recommendation['reasoning']}")
```

### Step 5.2: Cost Optimization

Review the monitoring dashboard views:
```sql
-- Which pipelines cost the most?
SELECT * FROM apex_production.observability.v_cost_by_pipeline
ORDER BY total_dbu_cost DESC LIMIT 20;

-- SLA compliance (are we over-provisioning?)
SELECT * FROM apex_production.observability.v_sla_compliance;

-- Candidates for serverless (short-running, bursty)
SELECT pipeline_name, avg_duration_sec, executions
FROM apex_production.observability.v_cost_by_pipeline
WHERE avg_duration_sec < 600  -- < 10 minutes → serverless candidate
ORDER BY total_dbu_cost DESC;
```

### Step 5.3: Idempotent Design Verification

```python
from aws2lakehouse.observability import IdempotentPatterns

# Verify all pipelines use idempotent patterns
merge_pattern = IdempotentPatterns.generate_merge_pattern(
    target_table="apex_production.risk_silver.trades",
    merge_keys=["trade_id", "trade_version"],
    partition_col="trade_date"
)
```

---

## Phase 6: Knowledge Transfer & Handoff (Weeks 22-24)

### Documentation Checklist
- [ ] All pipeline specs committed to Git
- [ ] Runbook for each domain (troubleshooting guide)
- [ ] Architecture diagrams updated
- [ ] Monitoring dashboard walkthrough recorded
- [ ] On-call rotation established
- [ ] CI/CD pipeline documented

### Training Topics
1. Writing pipeline YAML specs
2. Using the factory to generate artifacts
3. Interpreting monitoring dashboards
4. Adding new quality rules
5. MNPI controls and column masking
6. Debugging with GenAI assistant
7. CI/CD deployment process
8. Performance tuning (Spark UI + observability views)

---

## Appendix A: Troubleshooting

### Common Issues

| Problem | Cause | Solution |
|---------|-------|----------|
| "Table not found" | Missing catalog/schema | Run `uc.generate_setup_sql()` |
| Schema evolution fails | cloudFiles options | Add `mergeSchema=true` |
| Column mask not working | Missing function | Run governance SQL first |
| Checkpoint corruption | Schema changed | Delete checkpoint, restart |
| OOM on executor | Data skew | Repartition + salting |
| SLA breach | Insufficient compute | Upgrade to job cluster or add workers |

### Using the Debug Assistant

```python
from aws2lakehouse.genai import DebugAssistant

debugger = DebugAssistant()
diagnosis = debugger.diagnose(
    error_message="StreamingQueryException: schema mismatch at checkpoint",
    code=notebook_code  # optional
)

print(f"Error: {diagnosis['error_type']}")
print(f"Cause: {diagnosis['likely_cause']}")
print(f"Fix:   {diagnosis['suggested_fix']}")
```

---

## Appendix B: Pipeline YAML Reference

See [PIPELINE_YAML_REFERENCE.md](PIPELINE_YAML_REFERENCE.md) for the complete
specification format with all available options.
