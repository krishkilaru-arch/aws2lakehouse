# Target Architecture

> Specific to this migration: 1 pipelines across 1 domains

## Unity Catalog Structure

```
ecommerce_prod/                         ← Production catalog
├── analytics_bronze/              ← Raw ingestion (analytics)
├── analytics_silver/              ← Curated (analytics)
├── analytics_gold/                ← Serving (analytics)
├── _governance/              ← Audit, MNPI registry
└── _monitoring/              ← Pipeline metrics, DQ scores
```

## Schemas Created

  - `ecommerce_prod.analytics_bronze`
  - `ecommerce_prod.analytics_silver`
  - `ecommerce_prod.analytics_gold`

## Compute Strategy

| Pipeline Pattern | Compute Type | Pipelines |
|-----------------|-------------|-----------|
| Streaming (continuous) | Always-on cluster | None |
| High-frequency (<1hr) | Serverless | None |
| Daily batch | Job cluster (auto-terminate) | ecommerce_daily_etl |
| Event-triggered | Serverless (file arrival) | None |

## Data Flow

```
Sources                    Bronze              Silver              Gold
───────                    ──────              ──────              ────
delta_table               → analytics_bronze      → analytics_silver      → analytics_gold
```
