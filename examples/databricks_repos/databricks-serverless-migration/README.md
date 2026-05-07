# Acme-Capital Data Platform (Databricks)

Auto-generated from AWS source project using **aws2lakehouse** accelerator.

## Deploy

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

## Pipelines (5)

| Pipeline | Domain | Source | Classification |
|----------|--------|--------|----------------|
| bronze_to_silver | finance | auto_loader | internal |
| silver_to_gold | finance | auto_loader | internal |
| silver_transformation | finance | auto_loader | confidential |
| bronze_ingestion | finance | auto_loader | internal |
| gold_aggregation | finance | auto_loader | internal |

## Structure

- `src/pipelines/` — Ingestion notebooks (per domain)
- `resources/jobs/` — Job schedules and compute config
- `governance/` — Column masks, row filters, bootstrap SQL
- `quality/` — Data quality expectations
- `monitoring/` — Freshness SLA and volume anomaly queries
- `tests/` — Migration validation tests
