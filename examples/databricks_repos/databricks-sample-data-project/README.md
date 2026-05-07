# Acme-Capital Data Platform (Databricks)

Auto-generated from AWS source project using **aws2lakehouse** accelerator.

## Deploy

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

## Pipelines (8)

| Pipeline | Domain | Source | Classification |
|----------|--------|--------|----------------|
| daily_risk_aggregation | risk | delta_table | internal |
| loan_application_etl | lending | jdbc | confidential |
| customer_360 | customer | kafka | confidential |
| trade_events_streaming | risk | kafka | mnpi |
| customer_churn_model | analytics | delta_table | internal |
| market_data_feed | risk | kafka | mnpi |
| vendor_file_ingestion | finance | auto_loader | internal |
| payment_processing | lending | jdbc | confidential |

## Structure

- `src/pipelines/` — Ingestion notebooks (per domain)
- `resources/jobs/` — Job schedules and compute config
- `governance/` — Column masks, row filters, bootstrap SQL
- `quality/` — Data quality expectations
- `monitoring/` — Freshness SLA and volume anomaly queries
- `tests/` — Migration validation tests
