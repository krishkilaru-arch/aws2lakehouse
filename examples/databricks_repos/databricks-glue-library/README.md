# Acme-Capital Data Platform (Databricks)

Auto-generated from AWS source project using **aws2lakehouse** accelerator.

## Deploy

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

## Pipelines (9)

| Pipeline | Domain | Source | Classification |
|----------|--------|--------|----------------|
| feature_store_builder | finance | auto_loader | internal |
| customer_dimension | finance | jdbc | confidential |
| order_events_processing | finance | auto_loader | internal |
| financial_reconciliation | finance | jdbc | confidential |
| vendor_data_ingestion | finance | auto_loader | internal |
| market_data_ingest_job | finance | auto_loader | internal |
| position_snapshot_job | finance | jdbc | internal |
| risk_aggregation_job | finance | auto_loader | internal |
| nav_calculation_job | finance | jdbc | internal |

## Structure

- `src/pipelines/` — Ingestion notebooks (per domain)
- `resources/jobs/` — Job schedules and compute config
- `governance/` — Column masks, row filters, bootstrap SQL
- `quality/` — Data quality expectations
- `monitoring/` — Freshness SLA and volume anomaly queries
- `tests/` — Migration validation tests
