# Ingestion Patterns Used

> 4 distinct ingestion patterns across 8 pipelines

## Pattern Summary

| Source Type | Count | Pipelines |
|-------------|-------|-----------|
| auto_loader | 1 | vendor_file_ingestion |
| delta_table | 2 | daily_risk_aggregation, customer_churn_model |
| jdbc | 2 | loan_application_etl, payment_processing |
| kafka | 3 | customer_360, trade_events_streaming, market_data_feed |


## Pattern Details

### Auto Loader (1 pipelines)

**Pipelines:** vendor_file_ingestion

**Pattern:** See `src/pipelines/*/bronze/vendor_file_ingestion.py` for reference implementation.


### Delta Table (2 pipelines)

**Pipelines:** daily_risk_aggregation, customer_churn_model

**Pattern:** See `src/pipelines/*/bronze/daily_risk_aggregation.py` for reference implementation.


### Jdbc (2 pipelines)

**Pipelines:** loan_application_etl, payment_processing

**Pattern:** See `src/pipelines/*/bronze/loan_application_etl.py` for reference implementation.


### Kafka (3 pipelines)

**Pipelines:** customer_360, trade_events_streaming, market_data_feed

**Pattern:** See `src/pipelines/*/bronze/customer_360.py` for reference implementation.



## Common Configuration

All pipelines use:
- **Secrets:** `dbutils.secrets.get(scope, key)` — no hardcoded credentials
- **Parameterization:** `os.getenv("CATALOG")`, `os.getenv("SCHEMA")`, `os.getenv("TABLE")`
- **Audit columns:** `_ingested_at` (timestamp), `_source_file` (for file-based)
- **Schema evolution:** `mergeSchema=true` on all writes
- **Checkpointing:** `/Volumes/{catalog}/{schema}/_checkpoints/{pipeline}`
