# Target Architecture

> Specific to this migration: 5 pipelines across 1 domains

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
| Event-triggered | Serverless (file arrival) | bronze_to_silver, silver_to_gold, silver_transformation, bronze_ingestion, gold_aggregation |

## Data Flow

```
Sources                    Bronze              Silver              Gold
───────                    ──────              ──────              ────
auto_loader               → finance_bronze      → finance_silver      → finance_gold
auto_loader               → finance_bronze      → finance_silver      → finance_gold
auto_loader               → finance_bronze      → finance_silver      → finance_gold
auto_loader               → finance_bronze      → finance_silver      → finance_gold
auto_loader               → finance_bronze      → finance_silver      → finance_gold
```
