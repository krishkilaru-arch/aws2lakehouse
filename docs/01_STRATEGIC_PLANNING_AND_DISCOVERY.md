# 1. Strategic Planning & Discovery

## Purpose

This phase establishes the migration foundation: understanding what exists, who owns it, how critical it is, and in what order to migrate. **Skip this and you'll rework 40% of your migrations.**

---

## 1.1 Stakeholder Discovery Sessions

### Objectives
- Capture business priorities, SLAs, and success metrics across all domains
- Identify pipeline owners, data consumers, and downstream dependencies
- Uncover undocumented tribal knowledge about pipeline behavior

### Stakeholder Interview Template (24 Questions)

**Business Context (Questions 1-6):**
1. What business process does this pipeline support?
2. Who are the primary data consumers (dashboards, APIs, ML models, reports)?
3. What happens if this pipeline fails for 1 hour? 4 hours? 24 hours?
4. What is the agreed SLA (data freshness requirement)?
5. Are there regulatory or compliance requirements (MNPI, PII, SOX, GDPR)?
6. What is the current monthly cost attributed to this pipeline?

**Technical Context (Questions 7-12):**
7. What source systems feed this pipeline?
8. What is the data volume (daily rows, GB/day)?
9. What is the pipeline's schedule and trigger mechanism?
10. Are there custom JARs, Python libraries, or proprietary logic?
11. What orchestration tool is used (Airflow, Step Functions, cron, Glue Triggers)?
12. Is this pipeline stateful (checkpoints, watermarks, bookmarks)?

**Dependencies (Questions 13-18):**
13. What upstream pipelines must complete before this one starts?
14. What downstream pipelines depend on this one's output?
15. Are there cross-team dependencies (other teams read your output)?
16. Is there a data contract (schema, SLA, quality guarantees)?
17. What happens during a schema change from the source?
18. Is there a backfill process? How often is it needed?

**Migration Readiness (Questions 19-24):**
19. Has this pipeline been modified in the last 6 months?
20. Are there known issues or tech debt?
21. Is there a test suite or validation process?
22. What is the team's comfort level with Databricks?
23. Are there any hard blockers (vendor contracts, compliance freezes)?
24. What would "success" look like for this pipeline on Databricks?

### SLA Capture Form

| Pipeline | Owner | Freshness SLA | Completeness SLA | Availability SLA | Penalty |
|----------|-------|---------------|------------------|------------------|---------|
| trade_events | Risk Eng | 5 min | 99.9% | 99.99% | P1 incident |
| daily_risk_agg | Risk Eng | 6:00 AM ET | 100% (no partial) | 99.9% | Regulatory |
| customer_360 | Platform | 4 hours | 99% | 99.5% | Business impact |

---

## 1.2 Data Source Inventory

### Supported Source Types

| Source Type | Scanner Module | Auto-Detection | Example |
|-------------|---------------|----------------|---------|
| Kafka / MSK | KafkaScanner | Topics, consumer groups, offsets | trade-events-v2 |
| PostgreSQL / MySQL | PostgreSQLScanner | Tables, schemas, row counts | public.customers |
| MongoDB | MongoDBScanner | Collections, indexes, doc counts | analytics.user_sessions |
| Snowflake | SnowflakeScanner | Databases, schemas, tables | PROD.FINANCE.TRANSACTIONS |
| S3 Files (CSV/JSON/Parquet) | S3Scanner | Bucket prefixes, file patterns | s3://acme-landing/vendor/ |
| Delta Share | DeltaShareScanner | Shares, schemas, tables | partner_share.market_data |
| Airflow DAGs | AirflowScanner | DAG files or API | daily_risk_aggregation.py |
| Step Functions | StepFunctionScanner | JSON state machines | customer_360_pipeline.json |
| EMR Scripts | EMRScanner | spark-submit shell scripts | trade_events_streaming.sh |
| Glue Jobs | GlueScanner | Python ETL scripts | payment_processing.py |

### Inventory Schema

```python
pipeline_inventory = {
    "name": str,              # Unique pipeline identifier
    "domain": str,            # Business domain (risk, lending, customer, etc.)
    "source_type": str,       # kafka, jdbc, auto_loader, mongodb, snowflake, delta_share, delta_table
    "source_config": dict,    # Connection details (secrets, topics, tables)
    "schedule": str,          # Cron expression or "continuous" or "triggered"
    "sla_minutes": int,       # Maximum acceptable latency
    "business_impact": str,   # critical, high, medium, low
    "owner": str,             # Team email
    "classification": str,    # mnpi, confidential, internal, public
    "mnpi_columns": list,     # Columns containing MNPI data
    "pii_columns": list,      # Columns containing PII
    "complexity_score": int,  # 0-100 (auto-calculated)
    "wave": int,              # Migration wave assignment (auto-calculated)
    "dependencies": list,     # Upstream pipeline names
    "tables_written": list,   # Output tables
    "tables_read": list,      # Input tables (for dependency graph)
}
```

### Running the Inventory Builder

```python
from aws2lakehouse.discovery import PipelineInventory, AirflowScanner, MongoDBScanner

# Scan all sources
inventory = PipelineInventory()
inventory.scan_airflow(dag_folder="/path/to/dags/")
inventory.scan_step_functions(sf_folder="/path/to/step_functions/")
inventory.scan_emr_scripts(emr_folder="/path/to/emr/")
inventory.scan_glue_jobs(glue_folder="/path/to/glue/")

# Auto-classify
inventory.classify_all()  # Adds complexity_score + business_impact

# Export
inventory.to_delta_table("catalog.migration.pipeline_inventory")
inventory.to_csv("pipeline_inventory.csv")
```

---

## 1.3 Current-State Architecture Assessment

### What to Assess

| Component | Key Questions | Tool |
|-----------|--------------|------|
| EMR Clusters | Instance types, count, utilization, spot vs on-demand | ClusterMapper |
| Custom JARs | Dependencies, deprecated APIs, Hive coupling | JARAnalyzer |
| Python Libraries | Version conflicts, Glue-specific imports | CodeTransformer |
| Orchestration | Airflow version, custom operators, connections | AirflowConverter |
| Storage | S3 layout, partitioning, file formats, sizes | S3Scanner |
| Networking | VPC, security groups, cross-account access | Manual |
| Costs | EMR, Glue, S3, transfer, Redshift | ROICalculator |

### Architecture Assessment Template

```
Current State:
├── Compute: EMR 6.12 (M5.2xl master, R5.4xl workers × 6)
├── Storage: S3 (15TB, Parquet + JSON, ~200 prefixes)
├── Orchestration: MWAA Airflow 2.7 (45 DAGs, 12 connections)
├── Streaming: MSK (3 topics, 12 partitions each)
├── Database: PostgreSQL RDS (3 instances), MongoDB Atlas
├── Governance: None (IAM roles only)
├── Monitoring: CloudWatch (basic metrics), no DQ
└── CI/CD: None (manual deployments via AWS Console)
```

---

## 1.4 Complexity Classification

### Scoring Model (0-100)

| Factor | Weight | Criteria |
|--------|--------|----------|
| Custom JARs | 20 | 0=none, 10=standard, 20=proprietary |
| Dependencies | 15 | 0=standalone, 8=1-3 deps, 15=4+ deps |
| Streaming | 15 | 0=batch, 8=micro-batch, 15=continuous |
| Data Volume | 15 | 0=<1GB, 8=1-100GB, 15=>100GB/day |
| Transformations | 15 | 0=simple, 8=joins+agg, 15=ML/complex |
| Source Count | 10 | 0=single, 5=2-3, 10=4+ sources |
| Governance | 10 | 0=internal, 5=confidential, 10=MNPI |

### Classification Buckets

| Score | Category | Migration Strategy | Estimated Effort |
|-------|----------|-------------------|-----------------|
| 0-25 | Simple | Auto-generate, minimal review | 2-4 hours |
| 26-50 | Medium | Auto-generate, moderate review | 1-2 days |
| 51-75 | Complex | Generate + manual refinement | 3-5 days |
| 76-100 | Critical | Manual migration with SME | 1-2 weeks |

```python
from aws2lakehouse.discovery import ComplexityAnalyzer

analyzer = ComplexityAnalyzer()
for pipeline in inventory:
    pipeline["complexity_score"] = analyzer.score(pipeline)
    pipeline["category"] = analyzer.classify(pipeline["complexity_score"])
```

---

## 1.5 Wave-Based Migration Roadmap

### Wave Planning Principles

1. **Wave 0 (Pilot)**: 3-5 representative pipelines (1 simple, 1 medium, 1 complex)
2. **Wave 1**: Low-risk, high-value pipelines (prove ROI quickly)
3. **Wave 2-N**: Dependency-ordered, domain-grouped batches
4. **Final Wave**: Complex/critical pipelines (with lessons learned applied)

### Auto-Wave Assignment

```python
from aws2lakehouse.discovery import WavePlanner, DependencyGraph

graph = DependencyGraph(inventory)
graph.build()  # Builds DAG from tables_read/tables_written

planner = WavePlanner(graph, max_pipelines_per_wave=20)
waves = planner.assign_waves()

# Output:
# Wave 0: [trade_events_ingestion, customer_360, vendor_file_ingestion]
# Wave 1: [market_data_feed, order_flow_capture, loan_application_etl, ...]
# Wave 2: [daily_risk_aggregation, compliance_report, ...]  (depends on Wave 1)
# Wave 3: [customer_churn_model, ...]  (depends on Wave 2)
```

### Wave Planning Dashboard

The accelerator generates a Wave Planning Dashboard with:
- `v_inventory_summary`: Pipeline count by domain, complexity, classification
- `v_wave_progress`: Completion % per wave
- `v_risk_heatmap`: Business impact × complexity matrix
- `v_effort_burndown`: Estimated vs actual hours
- `v_dependencies`: Cross-wave dependency visualization

---

## 1.6 Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Migration velocity | 20 pipelines/week (after pilot) | Pipeline_inventory.status = 'migrated' |
| First-time success rate | >85% | Pipelines passing validation on first deploy |
| SLA compliance | 100% (no SLA regression) | Freshness monitoring vs SLA table |
| Cost reduction | >40% vs AWS baseline | ROICalculator monthly comparison |
| Quality improvement | >95% DQ pass rate | DQ expectations pass/fail ratio |
| Governance coverage | 100% | All tables tagged + masked |

---

## Deliverables Checklist

- [ ] Stakeholder interviews completed (all domain owners)
- [ ] Pipeline inventory populated (all sources scanned)
- [ ] Complexity scores calculated (all pipelines classified)
- [ ] Dependency graph built (no orphaned nodes)
- [ ] Wave plan defined (approved by stakeholders)
- [ ] Success metrics agreed (with measurement method)
- [ ] Pilot pipelines selected (1 per complexity tier)
- [ ] Timeline estimated (with confidence interval)
