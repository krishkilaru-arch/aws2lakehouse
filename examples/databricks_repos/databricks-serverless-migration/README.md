# Acme Capital — Databricks Lakehouse Migration

[![Databricks](https://img.shields.io/badge/Platform-Databricks-red?logo=databricks)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-blue)](https://delta.io)
[![Unity Catalog](https://img.shields.io/badge/Governance-Unity%20Catalog-green)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![License](https://img.shields.io/badge/License-Proprietary-lightgrey)]()

> **Classification: Internal — Acme Capital Engineering**

## Overview

This repository contains the production data platform for Acme Capital's **Finance** domain, migrated from an AWS Serverless architecture (Glue + Step Functions + Lambda) to a **Databricks Lakehouse** built on Unity Catalog, Delta Lake, and Databricks Asset Bundles (DAB).

The migration was executed using an AI-assisted methodology leveraging Claude via Databricks Foundation Model APIs, achieving full end-to-end pipeline parity across five core data pipelines comprising the Bronze → Silver → Gold medallion architecture.

### Migration Summary

| Dimension | Source (AWS) | Target (Databricks) |
|-----------|-------------|---------------------|
| **Ingestion** | S3 + Lambda (validate/enrich) | Volumes + Auto Loader |
| **Transformation** | AWS Glue (PySpark) | Databricks Notebooks (PySpark) |
| **Orchestration** | AWS Step Functions | Multi-task Jobs (DAB) |
| **Storage Format** | Parquet on S3 | Delta Lake (managed tables) |
| **Governance** | IAM + Lake Formation | Unity Catalog (3-level namespace) |
| **Catalog** | AWS Glue Data Catalog | `acme_prod.finance` |
| **Infrastructure-as-Code** | CloudFormation / CDK | Databricks Asset Bundles |

### Pipelines

| Pipeline | Layer | Complexity (tokens) | Description |
|----------|-------|-------------------|-------------|
| `bronze_ingestion` | Bronze | 4,050 | Raw data ingestion via Auto Loader with schema enforcement |
| `bronze_to_silver` | Silver | 6,512 | Cleansing, deduplication, and conformance transformations |
| `silver_transformation` | Silver | 4,378 | Business logic enrichment and derived column computation |
| `silver_to_gold` | Gold | 5,296 | Dimensional modeling and business-entity aggregation |
| `gold_aggregation` | Gold | 3,388 | Final reporting aggregates and KPI materialization |

---

## Architecture

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Databricks Lakehouse Platform                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────┐     ┌──────────────┐     ┌───────────────┐     ┌─────────┐ │
│   │  Volumes │────▶│ Auto Loader  │────▶│  Bronze Layer  │────▶│  Silver │ │
│   │  (Raw)   │     │ (Streaming)  │     │ (Delta Tables) │     │  Layer  │ │
│   └──────────┘     └──────────────┘     └───────────────┘     └────┬────┘ │
│                                                                      │      │
│                                                                      ▼      │
│   ┌──────────────┐     ┌──────────┐     ┌───────────────────────────────┐ │
│   │  Dashboards  │◀────│   Gold   │◀────│  silver_transformation        │ │
│   │  & Reports   │     │  Layer   │     │  silver_to_gold               │ │
│   └──────────────┘     └──────────┘     └───────────────────────────────┘ │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  Unity Catalog: acme_prod.finance.*          Asset Bundles: CI/CD & IaC     │
│  Row-Level Security | Column Masks | Tags    Multi-task Jobs | Monitoring   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Namespace Convention

All objects reside under the Unity Catalog three-level namespace:

```
acme_prod.finance.<table_name>
```

| Layer | Schema Pattern | Example |
|-------|---------------|---------|
| Bronze | `acme_prod.finance.bronze_*` | `acme_prod.finance.bronze_transactions` |
| Silver | `acme_prod.finance.silver_*` | `acme_prod.finance.silver_transactions_cleaned` |
| Gold | `acme_prod.finance.gold_*` | `acme_prod.finance.gold_daily_revenue` |

### Orchestration DAG

```
bronze_ingestion
       │
       ▼
bronze_to_silver
       │
       ├──────────────────┐
       ▼                  ▼
silver_transformation   (parallel branch if applicable)
       │
       ▼
silver_to_gold
       │
       ▼
gold_aggregation
```

Orchestration is defined declaratively in `resources/jobs/` as Databricks Asset Bundle YAML and deployed as a multi-task job with dependency chaining.

---

## Repository Structure

```
.
├── databricks.yml                          # Asset Bundle root configuration
├── src/
│   └── pipelines/
│       └── finance/
│           ├── bronze/
│           │   └── bronze_ingestion.py     # Auto Loader ingestion pipeline
│           ├── silver/
│           │   ├── bronze_to_silver.py     # Cleansing & conformance
│           │   └── silver_transformation.py # Business logic enrichment
│           └── gold/
│               ├── silver_to_gold.py       # Dimensional modeling
│               └── gold_aggregation.py     # KPI materialization
├── resources/
│   └── jobs/
│       ├── bronze_ingestion_job.yml
│       ├── bronze_to_silver_job.yml
│       ├── silver_transformation_job.yml
│       ├── silver_to_gold_job.yml
│       ├── gold_aggregation_job.yml
│       └── finance_orchestration_job.yml   # End-to-end DAG
├── governance/
│   ├── tags.yml                            # Sensitivity & classification tags
│   ├── column_masks.sql                    # Dynamic column masking policies
│   └── row_filters.sql                     # Row-level security filters
├── quality/
│   └── expectations.yml                    # Delta Live Tables / DQ expectations
├── monitoring/
│   ├── freshness_checks.yml               # SLA-based freshness monitoring
│   ├── volume_checks.yml                  # Row count anomaly detection
│   └── schema_drift.yml                   # Schema evolution alerting
└── tests/
    └── validation/
        ├── test_row_counts.py             # Source-target row parity
        ├── test_schema_compatibility.py   # Schema regression tests
        ├── test_data_quality.py           # DQ expectation assertions
        └── conftest.py                    # Pytest fixtures & Spark session
```

---

## Quick Start

### Prerequisites

| Requirement | Version | Purpose |
|-------------|---------|---------|
| Databricks CLI | ≥ 0.218 | Asset Bundle deployment |
| Python | 3.10+ | Local development & testing |
| Databricks Workspace | Premium+ | Unity Catalog support |
| Access | `acme_prod` catalog | Read/write permissions |

### 1. Clone & Configure

```bash
git clone https://github.com/acme-capital/finance-lakehouse.git
cd finance-lakehouse

# Authenticate to Databricks workspace
databricks auth login --host https://<workspace-url>

# Verify connectivity
databricks clusters list
```

### 2. Validate the Bundle

```bash
databricks bundle validate
```

This performs a dry-run validation of all job definitions, cluster policies, and resource references.

### 3. Deploy to Development

```bash
databricks bundle deploy --target dev
```

### 4. Run the Orchestration Job

```bash
databricks bundle run finance_orchestration_job --target dev
```

---

## Deployment

### Environment Targets

The Asset Bundle (`databricks.yml`) defines three deployment targets aligned with Acme Capital's SDLC:

| Target | Catalog | Workspace | Approval Required |
|--------|---------|-----------|-------------------|
| `dev` | `acme_dev` | Development | No |
| `staging` | `acme_staging` | Staging | Peer review |
| `prod` | `acme_prod` | Production | Change Advisory Board |

### CI/CD Pipeline

```
┌────────┐     ┌──────────┐     ┌─────────┐     ┌────────────┐
│  Push  │────▶│ Validate │────▶│  Test   │────▶│  Deploy    │
│  (PR)  │     │  Bundle  │     │ (pytest)│     │ (staging)  │
└────────┘     └──────────┘     └─────────┘     └─────┬──────┘
                                                       │
                                                       ▼
                                              ┌────────────────┐
                                              │ Deploy (prod)  │
                                              │ (manual gate)  │
                                              └────────────────┘
```

### Deployment Commands

```bash
# Development (automatic on feature branch)
databricks bundle deploy --target dev

# Staging (on merge to main)
databricks bundle deploy --target staging

# Production (manual approval + tag-based release)
databricks bundle deploy --target prod
```

### Asset Bundle Configuration

```yaml
# databricks.yml (simplified)
bundle:
  name: finance-lakehouse

workspace:
  host: ${var.workspace_url}

targets:
  dev:
    default: true
    variables:
      catalog: acme_dev
      schema: finance
  staging:
    variables:
      catalog: acme_staging
      schema: finance
  prod:
    variables:
      catalog: acme_prod
      schema: finance
    run_as:
      service_principal_name: sp-finance-prod
```

---

## Governance

### Unity Catalog Controls

All data assets are governed under Unity Catalog with the following controls enforced:

#### Data Classification Tags

| Tag | Applied To | Purpose |
|-----|-----------|---------|
| `pii` | Columns containing personal data | Triggers masking policies |
| `confidential` | Tables with restricted access | Audit trail enforcement |
| `regulatory_reporting` | Gold tables used in filings | Retention & lineage |
| `spi` | Sensitive personal information | Enhanced access controls |

#### Column Masking

Dynamic column masks are applied based on group membership:

```sql
-- governance/column_masks.sql
CREATE FUNCTION acme_prod.finance.mask_account_number(account_number STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('finance_analysts') THEN account_number
  ELSE CONCAT('****', RIGHT(account_number, 4))
END;
```

#### Row-Level Security

```sql
-- governance/row_filters.sql
CREATE FUNCTION acme_prod.finance.region_filter(region STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('global_finance') THEN TRUE
  WHEN is_member('us_finance') AND region = 'US' THEN TRUE
  ELSE FALSE
END;
```

### Lineage & Audit

- **Lineage**: Automatically captured by Unity Catalog at column level across all pipeline stages.
- **Audit Logs**: All data access events are streamed to the Acme Capital SIEM via system tables (`system.access.audit`).
- **Retention**: Gold-layer regulatory tables are retained for 7 years per SEC/FINRA requirements.

---

## Data Quality

### Expectation Framework

Data quality expectations are defined declaratively and enforced at each layer boundary:

```yaml
# quality/expectations.yml
bronze_ingestion:
  - expectation: "transaction_id IS NOT NULL"
    action: drop
    severity: critical
  - expectation: "amount > 0"
    action: quarantine
    severity: warning

bronze_to_silver:
  - expectation: "COUNT(*) > 0"
    action: fail
    severity: critical
  - expectation: "duplicate_ratio < 0.01"
    action: alert
    severity: warning
```

### Quality Tiers

| Layer | Enforcement | On Failure |
|-------|------------|------------|
| Bronze | Schema validation, NOT NULL on keys | Quarantine to `_quarantine` table |
| Silver | Business rules, referential integrity | Fail task, alert on-call |
| Gold | Aggregate bounds, completeness | Block publish, notify stakeholders |

---

## Monitoring

### Operational Monitoring

Three monitoring dimensions are tracked continuously:

#### 1. Data Freshness

```yaml
# monitoring/freshness_checks.yml
checks:
  - table: acme_prod.finance.gold_daily_revenue
    max_age_minutes: 120
    alert_channel: "#finance-data-ops"
    escalation: pagerduty
```

#### 2. Volume Anomaly Detection

```yaml
# monitoring/volume_checks.yml
checks:
  - table: acme_prod.finance.bronze_transactions
    expected_daily_rows: 500000
    tolerance_pct: 25
    alert_on: [spike, drop, zero]
```

#### 3. Schema Drift

```yaml
# monitoring/schema_drift.yml
checks:
  - table: acme_prod.finance.silver_transactions_cleaned
    alert_on: [column_added, column_removed, type_changed]
    auto_evolve: false  # Require manual approval in prod
```

### Alerting Matrix

| Severity | Response Time | Channel | Escalation |
|----------|--------------|---------|------------|
| Critical | 15 min | PagerDuty | Engineering Manager |
| High | 1 hour | Slack `#finance-data-ops` | Team Lead |
| Medium | 4 hours | Email | Sprint backlog |
| Low | Next sprint | Jira | Backlog |

---

## Testing

### Test Suite

The validation test suite ensures migration parity and ongoing correctness:

```bash
# Run all validation tests
pytest tests/validation/ -v --tb=short

# Run specific test category
pytest tests/validation/test_row_counts.py -v
pytest tests/validation/test_schema_compatibility.py -v
pytest tests/validation/test_data_quality.py -v
```

### Test Categories

| Test File | Purpose | Frequency |
|-----------|---------|-----------|
| `test_row_counts.py` | Source-target row count parity (±0.1%) | Post-deployment |
| `test_schema_compatibility.py` | Schema regression against baseline | Every PR |
| `test_data_quality.py` | DQ expectation pass rates ≥ 99.5% | Daily (scheduled) |

### Migration Validation Criteria

The following acceptance criteria were validated during migration:

| Criterion | Threshold | Status |
|-----------|-----------|--------|
| Row count parity (source vs. target) | ≥ 99.9% | ✅ Passed |
| Schema compatibility | 100% column mapping | ✅ Passed |
| Data quality expectations | ≥ 99.5% pass rate | ✅ Passed |
| End-to-end latency | ≤ source SLA | ✅ Passed |
| Orchestration dependency parity | 1:1 DAG mapping | ✅ Passed |

### Running Tests Locally

```bash
# Install test dependencies
pip install -r requirements-dev.txt

# Configure Databricks Connect for local Spark session
databricks-connect configure

# Execute tests
pytest tests/validation/ \
  --catalog=acme_dev \
  --schema=finance \
  -v
```

---

## Operations Runbook

### Common Operations

| Task | Command |
|------|---------|
| Redeploy single pipeline | `databricks bundle deploy --target prod --resource bronze_ingestion_job` |
| Trigger manual backfill | `databricks bundle run finance_orchestration_job --target prod --params '{"backfill_date": "2024-01-15"}'` |
| Check job run status | `databricks jobs list-runs --job-id <id> --limit 5` |
| View Unity Catalog lineage | Workspace → Catalog Explorer → `acme_prod.finance` → Lineage tab |
| Rotate service principal | Update `sp-finance-prod` credentials in Databricks Secrets |

### Incident Response

1. **Pipeline Failure**: Check job run logs → Identify failed task → Review error in notebook output
2. **Data Quality Alert**: Query `_quarantine` table → Assess impact → Notify data steward
3. **Schema Drift Detected**: Review Auto Loader schema evolution log → Approve or reject via PR
4. **Freshness SLA Breach**: Check upstream dependencies → Verify cluster availability → Escalate if infrastructure

---

## Contributing

### Branch Strategy

| Branch | Purpose | Deploys To |
|--------|---------|-----------|
| `feature/*` | New development | `dev` (auto) |
| `main` | Integration | `staging` (auto) |
| `release/*` | Production releases | `prod` (manual gate) |

### Pull Request Requirements

- [ ] `databricks bundle validate` passes
- [ ] All pytest tests pass
- [ ] Governance tags applied to new tables/columns
- [ ] DQ expectations defined for new pipelines
- [ ] Monitoring thresholds configured
- [ ] Peer review by ≥ 1 Finance Data Engineering team member
- [ ] Security review for PII/SPI changes

---

## Contacts

| Role | Team | Channel |
|------|------|---------|
| Data Engineering | Finance Platform | `#finance-data-eng` |
| Data Governance | Enterprise Data Office | `#data-governance` |
| Platform Support | Databricks Admin | `#platform-support` |
| On-Call | Rotating | PagerDuty `finance-data-ops` |

---

## License

**Proprietary — Acme Capital, Inc.**

This repository contains confidential and proprietary information. Unauthorized reproduction, distribution, or disclosure is strictly prohibited. All rights reserved.

---

*Last updated: 2024 | Maintained by Finance Data Engineering | Databricks Asset Bundles v0.218+*