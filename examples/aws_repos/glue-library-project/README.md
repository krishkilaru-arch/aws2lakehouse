# Acme ETL Platform (AWS Glue + Shared Library)

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  S3: acme-libs/acme_etl-3.2.1.whl  (shared library)     │
└────────────────────────┬─────────────────────────────────┘
                         │ --extra-py-files
    ┌────────────────────┼────────────────────────────┐
    │                    │                            │
    ▼                    ▼                            ▼
┌──────────┐    ┌────────────────┐    ┌───────────────────────┐
│ customer │    │ order_events   │    │ financial_recon       │
│ dimension│    │ processing     │    │                       │
│ (SCD2)   │    │ (flatten+enrich│    │ (3-way match)        │
└──────────┘    └────────────────┘    └───────────────────────┘
    │                    │                            │
    ▼                    ▼                            ▼
┌──────────────────────────────────────────────────────────┐
│  S3: acme-data-lake/ (bronze / silver / gold)            │
└──────────────────────────────────────────────────────────┘
```

## Library Modules (`lib/acme_etl/`)

| Module | Purpose |
|--------|---------|
| transforms/scd2 | SCD Type 2 merge (detect_changes, scd2_merge) |
| transforms/dedup | Deduplication (by key, by window, merge strategy) |
| transforms/flatten | Nested JSON/struct flattening, array explosion |
| transforms/enrichment | Lookup joins, broadcast enrichment, geo enrichment |
| transforms/aggregations | Running totals, period summaries |
| quality/checks | Quality framework (not_null, unique, range, freshness) |
| io/readers | Universal reader (S3, JDBC, Kafka) with secret management |
| io/writers | Universal writer (S3, Redshift) with partitioning |
| utils/masking | PII masking (hash, partial, redact, tokenize) |
| utils/keys | Surrogate + hash key generation |
| utils/audit | Audit logging to DynamoDB |

## Deploy

```bash
cd deploy/
./build_wheel.sh
```

## Jobs

| Job | Schedule | Workers | Source | Target |
|-----|----------|---------|--------|--------|
| customer_dimension | Daily 2am | 10 × G.2X | PostgreSQL | S3 gold/ |
| order_events_processing | Hourly | 5 × G.1X | S3 JSON | S3 silver/ |
| financial_reconciliation | Daily 7am | 8 × G.2X | S3 + PostgreSQL | S3 gold/ |
| feature_store_builder | Weekly Sun | 20 × G.2X | S3 silver+gold | S3 features/ |
| vendor_data_ingestion | Every 15min | 3 × G.1X | S3 multi-format | S3 bronze/ |
