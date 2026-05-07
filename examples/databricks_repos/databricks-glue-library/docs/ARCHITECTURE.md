# Target Architecture

> Specific to this migration: 9 pipelines across 1 domains

## Unity Catalog Structure

```
acme_prod/                         ← Production catalog
├── finance_bronze/              ← Raw ingestion (finance)
├── finance_silver/              ← Curated (finance)
├── finance_gold/                ← Serving (finance)
├── _governance/              ← Audit, MNPI registry
└── _monitoring/              ← Pipeline metrics, DQ scores
```

## Schemas Created

  - `acme_prod.finance_bronze`
  - `acme_prod.finance_silver`
  - `acme_prod.finance_gold`

## Compute Strategy

| Pipeline Pattern | Compute Type | Pipelines |
|-----------------|-------------|-----------|
| Streaming (continuous) | Always-on cluster | None |
| High-frequency (<1hr) | Serverless | None |
| Daily batch | Job cluster (auto-terminate) | None |
| Event-triggered | Serverless (file arrival) | feature_store_builder, customer_dimension, order_events_processing, financial_reconciliation, vendor_data_ingestion, market_data_ingest_job, position_snapshot_job, risk_aggregation_job, nav_calculation_job |

## Data Flow

```
Sources                    Bronze              Silver              Gold
───────                    ──────              ──────              ────
auto_loader               → finance_bronze      → finance_silver      → finance_gold
jdbc                      → finance_bronze      → finance_silver      → finance_gold
auto_loader               → finance_bronze      → finance_silver      → finance_gold
jdbc                      → finance_bronze      → finance_silver      → finance_gold
auto_loader               → finance_bronze      → finance_silver      → finance_gold
auto_loader               → finance_bronze      → finance_silver      → finance_gold
```
