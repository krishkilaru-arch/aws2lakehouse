# Acme-Retail Data Platform (Databricks)

Auto-generated from AWS source project using **aws2lakehouse** accelerator.

## Deploy

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

## Pipelines (1)

| Pipeline | Domain | Source | Classification |
|----------|--------|--------|----------------|
| ecommerce_daily_etl | analytics | delta_table | internal |

## Structure

- `src/pipelines/` — Ingestion notebooks (per domain)
- `resources/jobs/` — Job schedules and compute config
- `governance/` — Column masks, row filters, bootstrap SQL
- `quality/` — Data quality expectations
- `monitoring/` — Freshness SLA and volume anomaly queries
- `tests/` — Migration validation tests
