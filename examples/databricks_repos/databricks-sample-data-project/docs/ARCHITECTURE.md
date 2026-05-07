# Target Architecture

> Specific to this migration: 8 pipelines across 5 domains

## Unity Catalog Structure

```
acme_prod/                         ← Production catalog
├── analytics_bronze/              ← Raw ingestion (analytics)
├── customer_bronze/              ← Raw ingestion (customer)
├── finance_bronze/              ← Raw ingestion (finance)
├── lending_bronze/              ← Raw ingestion (lending)
├── risk_bronze/              ← Raw ingestion (risk)
├── analytics_silver/              ← Curated (analytics)
├── customer_silver/              ← Curated (customer)
├── finance_silver/              ← Curated (finance)
├── lending_silver/              ← Curated (lending)
├── risk_silver/              ← Curated (risk)
├── analytics_gold/                ← Serving (analytics)
├── customer_gold/                ← Serving (customer)
├── finance_gold/                ← Serving (finance)
├── lending_gold/                ← Serving (lending)
├── risk_gold/                ← Serving (risk)
├── _governance/              ← Audit, MNPI registry
└── _monitoring/              ← Pipeline metrics, DQ scores
```

## Schemas Created

  - `acme_prod.analytics_bronze`
  - `acme_prod.analytics_silver`
  - `acme_prod.analytics_gold`
  - `acme_prod.customer_bronze`
  - `acme_prod.customer_silver`
  - `acme_prod.customer_gold`
  - `acme_prod.finance_bronze`
  - `acme_prod.finance_silver`
  - `acme_prod.finance_gold`
  - `acme_prod.lending_bronze`
  - `acme_prod.lending_silver`
  - `acme_prod.lending_gold`
  - `acme_prod.risk_bronze`
  - `acme_prod.risk_silver`
  - `acme_prod.risk_gold`

## Compute Strategy

| Pipeline Pattern | Compute Type | Pipelines |
|-----------------|-------------|-----------|
| Streaming (continuous) | Always-on cluster | trade_events_streaming, market_data_feed |
| High-frequency (<1hr) | Serverless | loan_application_etl, payment_processing |
| Daily batch | Job cluster (auto-terminate) | daily_risk_aggregation, customer_360, customer_churn_model |
| Event-triggered | Serverless (file arrival) | vendor_file_ingestion |

## Data Flow

```
Sources                    Bronze              Silver              Gold
───────                    ──────              ──────              ────
delta_table               → risk_bronze      → risk_silver      → risk_gold
jdbc                      → lending_bronze      → lending_silver      → lending_gold
kafka                     → customer_bronze      → customer_silver      → customer_gold
kafka                     → risk_bronze      → risk_silver      → risk_gold
delta_table               → analytics_bronze      → analytics_silver      → analytics_gold
kafka                     → risk_bronze      → risk_silver      → risk_gold
```
